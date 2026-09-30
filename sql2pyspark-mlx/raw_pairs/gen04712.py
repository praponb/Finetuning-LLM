from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull() & (F.col("channel") == "web"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)
