from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.count("*").alias("row_count"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
    )
)
