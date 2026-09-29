from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel").isin(["store", "partner", "mobile"]))
    .groupBy(
        F.col("c.city"),
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.count(F.when(F.col("p.method") == "card", 1)).alias("card_count"))
    .orderBy("signup_cohort")
)
