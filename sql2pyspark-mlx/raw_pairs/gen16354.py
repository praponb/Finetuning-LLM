from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel", "promo_code")
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("promo_code").asc_nulls_first())
)
