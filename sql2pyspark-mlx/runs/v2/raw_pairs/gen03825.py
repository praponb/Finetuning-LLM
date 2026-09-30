from pyspark.sql import functions as F

recent_orders = df_orders.filter((F.col("status") == "COMPLETED") & (F.col("discount") > 0.36))

orders_stats = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    orders_stats
    .filter(F.col("partner_count") > 3)
    .select("channel", "partner_count", "min_amount")
    .orderBy(F.col("partner_count").desc(), "channel")
    .limit(5)
)
