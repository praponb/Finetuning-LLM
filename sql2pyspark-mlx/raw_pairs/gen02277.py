from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
    .orderBy("status_values", F.col("signup_year").desc())
    .limit(20)
)
