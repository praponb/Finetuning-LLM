from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct_rounded").desc(), "id")
w2 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy(F.col("discount_pct_rounded").desc_nulls_first())
w3 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w5 = Window.partitionBy("promo_code").orderBy("row_seq", "id")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct_rounded", "id")
w_grp = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-08-22").cast("date")) & F.month("starts_on").isin([1, 6, 8]))
    .withColumn("discount_pct_rounded", F.round("discount_pct", 2))
    .withColumn("ends_on_plus_17d", F.date_add("ends_on", 17))
)

with_windows = (
    filtered
    .filter(F.col("promo_code") == "VIP")
    .select(
        "*",
        F.ntile(5).over(w).alias("discount_pct_rounded_bucket"),
        F.cume_dist().over(w2).alias("discount_pct_rounded_cume_dist"),
        F.first("discount_pct_rounded", ignorenulls=True).over(w3).alias("first_known_discount_pct_rounded"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("ends_on").between("2023-04-01", "2025-08-26"))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct_rounded",
        "ends_on_plus_17d",
        "first_known_discount_pct_rounded",
        "row_seq",
        F.concat_ws("-", "promo_code", "channel").alias("promo_code_channel_key"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.row_number().over(w5).alias("row_seq_2"),
        F.row_number().over(w6).alias("row_seq_3"),
    )
)

scored_promotions = (
    scored
    .select(
        "id",
        "promo_code",
        "discount_pct_rounded",
        "first_known_discount_pct_rounded",
        "promo_code_channel_key",
        F.sum("first_known_discount_pct_rounded").over(w_grp).alias("last4_sum_first_known_discount_pct_rounded"),
        F.avg("first_known_discount_pct_rounded").over(w_grp).alias("last4_avg_first_known_discount_pct_rounded"),
        F.max("first_known_discount_pct_rounded").over(w_grp).alias("last4_max_first_known_discount_pct_rounded"),
    )
)

result = (
    scored_promotions
    .select(
        "id",
        "discount_pct_rounded",
        "first_known_discount_pct_rounded",
        "promo_code_channel_key",
    )
)
