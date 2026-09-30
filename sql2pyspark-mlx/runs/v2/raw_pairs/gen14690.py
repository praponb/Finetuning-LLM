from pyspark.sql import functions as F

src = df_orders.filter(F.col("order_date") <= "2023-03-22")

status_summary = (
    src
    .groupBy("status", "customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)

result = status_summary.select("status", "customer_id", "pct_shipped")
