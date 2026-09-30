from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.avg(F.col("a.balance")).alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("a.status")))).alias("status_values"),
    )
    .orderBy(F.col("status_values").desc(), "is_active", "signup_year")
    .limit(5)
)
