from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.month("starts_on"), "promo_code").orderBy(F.col("months_ends_on_to_starts_on").desc())
w_grp = Window.orderBy(F.col("months_ends_on_to_starts_on").desc(), "id")
w_ordered = Window.partitionBy("channel").orderBy("months_ends_on_to_starts_on", "id")
w4 = Window.partitionBy(F.month("starts_on"), "channel").orderBy("row_seq", "id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("months_ends_on_to_starts_on_dense_rank").desc(), "id")
w6 = Window.partitionBy("promo_code", "channel").orderBy(F.col("months_ends_on_to_starts_on_dense_rank_bucket").desc(), "id")
w7 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code", "channel").orderBy("row_num", "id").rowsBetween(-3, -1)
w9 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("channel", "promo_code").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.round((F.datediff("starts_on", "ends_on") / 30), 1).alias("months_ends_on_to_starts_on"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

metrics = (
    valid
    .filter(F.col("promo_code") == "VIP")
    .withColumn("months_ends_on_to_starts_on_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

with_windows = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "months_ends_on_to_starts_on_dense_rank",
        F.row_number().over(w4).alias("event_seq_2"),
        F.ntile(10).over(w5).alias("months_ends_on_to_starts_on_dense_rank_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "months_ends_on_to_starts_on_dense_rank",
        "event_seq_2",
        "row_num",
        F.sum("event_seq_2").over(w7).alias("running_event_seq_2"),
        F.avg("discount_pct").over(w8).alias("discount_pct_avg_3_prior"),
        F.last("event_seq_2", ignorenulls=True).over(w9).alias("last_known_event_seq_2"),
        F.first("months_ends_on_to_starts_on_dense_rank", ignorenulls=True).over(w10).alias("first_known_months_ends_on_to_starts_on_dense_rank"),
    )
)
