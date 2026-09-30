from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("shipping_fee").between(30, 39))
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
)

orders_stats = (
    valid_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(F.max("order_date").alias("last_order_date"))
)

result = (
    orders_stats
    .select("order_cohort", "last_order_date")
    .orderBy(F.col("last_order_date").desc())
)
