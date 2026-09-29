from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)
