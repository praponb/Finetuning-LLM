from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([8, 9, 10, 20]))
    .agg(
        F.count("*").alias("row_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
    )
    .orderBy(F.col("row_count").desc(), "pct_completed")
)
