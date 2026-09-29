from pyspark.sql import functions as F

result = (
    df_accounts
    .cube("account_type", "customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
