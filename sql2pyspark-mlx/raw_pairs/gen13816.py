from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.col("c.segment"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.count("*").alias("order_item_count"),
    )
    .orderBy(F.col("order_item_count").desc(), "signup_cohort", "segment")
    .limit(10)
)
