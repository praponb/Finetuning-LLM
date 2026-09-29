from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on_day", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("discount_pct").between(0.15, 0.17))
    .select(
        "*",
        F.dayofmonth("starts_on").alias("starts_on_day"),
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.15, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    base
    .filter(F.year("starts_on") == 2024)
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "starts_on_day",
        "channel_filled",
        F.ntile(3).over(w).alias("starts_on_day_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
