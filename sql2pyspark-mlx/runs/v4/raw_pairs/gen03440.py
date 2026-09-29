from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.when(F.year(F.col("customers.signup_date")) < 2024, "before_2024").when(F.year(F.col("customers.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("subscriptions.plan")))).alias("plan_values"),
        F.min(F.col("subscriptions.end_date")).alias("first_end_date"),
    )
)
