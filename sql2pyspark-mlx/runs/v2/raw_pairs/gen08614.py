from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("c.credit_limit") <= 8600) & F.col("c.is_active"))
    .select(
        F.col("c.signup_date"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.when(F.col("c.age") >= 62, "High").when(F.col("c.age") >= 28, "Medium").otherwise("Low").alias("age_band"),
    )
    .orderBy("signup_date", F.col("channel").desc())
)
