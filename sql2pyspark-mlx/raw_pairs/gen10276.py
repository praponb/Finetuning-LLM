from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id").isin([27, 29, 34]))
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.col("c.is_active"),
    )
    .agg(
        F.count(F.col("o.amount")).alias("amount_count"),
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
    )
    .filter(F.col("amount_count") > 5)
    .orderBy(F.col("signup_cohort").desc())
)
