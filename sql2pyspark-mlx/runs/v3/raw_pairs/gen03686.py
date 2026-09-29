from pyspark.sql import functions as F

recent_orders = df_orders.filter(~F.col("status").isin(["PENDING", "COMPLETED"]))

agg = (
    recent_orders
    .groupBy(F.month("order_date").alias("order_month"), "status")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    agg
    .orderBy("status", "order_month")
    .limit(50)
)
