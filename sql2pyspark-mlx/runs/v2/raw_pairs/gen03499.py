from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("a.account_type")))).alias("account_type_values"),
        F.min(F.col("a.opened_date")).alias("min_opened_date"),
    )
)
