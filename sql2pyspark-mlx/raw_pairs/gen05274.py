from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-08-01")))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.length("promo_code").alias("promo_code_length"),
        F.round((F.col("discount_pct") * 0.9), 2).alias("discount_pct_adjusted"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.min("discount_pct_adjusted").over(w).alias("running_min_discount_pct_adjusted"),
        F.avg("discount_pct_adjusted").over(w).alias("running_avg_discount_pct_adjusted"),
        F.count("discount_pct_adjusted").over(w).alias("running_count_discount_pct_adjusted"),
    )
)
