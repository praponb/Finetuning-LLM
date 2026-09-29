from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") == "Tokyo")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.year(F.col("c.signup_date")).alias("signup_year"),
    )
    .agg(F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"))
    .filter(F.col("revenue") >= 51)
    .orderBy("signup_year", "signup_cohort")
    .limit(10)
)
