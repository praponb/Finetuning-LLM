from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 6)
    .orderBy("status", "shipped_count")
)
