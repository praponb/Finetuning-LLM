from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") < 66)
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("a.status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("a.account_type")))).alias("account_type_values"),
    )
)
