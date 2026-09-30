from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store", "web"]) & F.col("order_date").between("2023-11-01", "2024-10-01"))
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
        F.count("*").alias("order_count"),
    )
)
