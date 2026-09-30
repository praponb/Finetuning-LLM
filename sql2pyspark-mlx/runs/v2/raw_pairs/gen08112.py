from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count("*").alias("order_count"),
        F.countDistinct("discount").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 9)
    .select("status", "order_count", "unique_stores")
    .orderBy("order_count", "status")
    .limit(5)
)
