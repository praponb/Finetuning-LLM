from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([6, 11, 12]) & F.col("balance").isNull())
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .orderBy("status", "account_type_values")
)
