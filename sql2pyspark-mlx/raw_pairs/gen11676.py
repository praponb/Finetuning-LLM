from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("amount").alias("unique_customers"),
    )
    .filter(F.col("total_count") >= 10)
    .select("channel", "total_count", "unique_customers")
    .orderBy(F.col("total_count").desc())
)
