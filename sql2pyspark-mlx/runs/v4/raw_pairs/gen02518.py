from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc())

latest = (
    df_promotions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("discount_pct") >= 0.28, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy(F.col("mobile_rows").desc_nulls_last())
)
