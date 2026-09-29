from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("customer_id").isin([4, 16, 20, 29]))
    .withColumn("years_since_order_date", (F.datediff(F.current_date(), "order_date") / 365.25))
)

agg = (
    filtered_orders
    .groupBy(
        F.when(F.col("discount") >= 0.43, "high").when(F.col("discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(F.avg("years_since_order_date"), 2).alias("avg_years_since_order_date"),
    )
)

result = (
    agg
    .filter(F.col("avg_years_since_order_date") < 67)
    .select("discount_bucket", "avg_years_since_order_date")
)
