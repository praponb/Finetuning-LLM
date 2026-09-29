from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 560)
    .groupBy("channel", "customer_id")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.countDistinct("discount").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") >= 8)
    .select("channel", "customer_id", "returned_count", "unique_stores")
    .orderBy("unique_stores")
)
