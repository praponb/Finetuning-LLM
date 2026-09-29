from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("status").isin(["COMPLETED", "SHIPPED"]))

summary = (
    valid_orders
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    summary
    .filter(F.col("pct_partner") < 56)
    .select("status", "pct_partner", "last_order_date")
    .orderBy("pct_partner", "status")
)
