from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")

ranked = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.27, "PREMIUM").otherwise("STANDARD"))
    .cube("discount_pct_tier", "channel")
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct_2"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count("*").alias("cnt"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_pct_per_mobile"),
        F.grouping_id().alias("grouping_id"),
    )
)
