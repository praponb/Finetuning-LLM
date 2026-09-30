from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNull())
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("pct_completed") < 77)
    .orderBy("last_order_date")
)
