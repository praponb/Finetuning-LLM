from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("prev_discount_pct", "id")

valid = (
    df_promotions
    .filter(F.col("ends_on") > "2023-04-01")
    .withColumn("starts_on_month", F.month("starts_on"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

cleaned = (
    valid
    .filter(F.col("channel").isNotNull())
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .filter(F.col("discount_pct_int") > 68)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "starts_on_month",
        "channel_filled",
        F.lag("starts_on_month").over(w2).alias("prev_starts_on_month"),
        F.lead("starts_on_month").over(w2).alias("next_starts_on_month"),
        F.lead("discount_pct").over(w2).alias("next_discount_pct"),
        F.lag("discount_pct").over(w2).alias("prev_discount_pct"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lead("starts_on_month").over(w3).alias("next_starts_on_month_2"),
        F.lag("prev_discount_pct").over(w3).alias("prev_prev_discount_pct"),
        F.lead("prev_discount_pct").over(w3).alias("next_prev_discount_pct"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ends_on",
        "starts_on_month",
        "prev_starts_on_month",
        "next_discount_pct",
        "prev_discount_pct",
        "row_seq",
        "next_starts_on_month_2",
        "prev_prev_discount_pct",
    )
    .orderBy(F.col("next_starts_on_month_2").desc_nulls_last())
)
