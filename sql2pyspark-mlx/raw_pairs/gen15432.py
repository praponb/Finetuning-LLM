from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("order_date") >= "2025-09-01")
    .withColumn("weeks_since_order_date", (F.datediff(F.current_date(), "order_date") / 7))
)

agg = (
    recent_orders
    .groupBy(
        "channel",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"))
)

result = (
    agg
    .filter(F.col("avg_shipping_fee") < 60)
    .orderBy(F.col("avg_shipping_fee").desc(), "channel", "order_cohort")
    .limit(5)
)
