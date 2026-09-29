from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.month(F.col("c.signup_date")).alias("signup_month"),
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "COMPLETED", F.col("o.amount")).otherwise(0)).alias("completed_amount"),
    )
    .filter(F.col("completed_amount") > 20)
    .orderBy("signup_cohort")
)
