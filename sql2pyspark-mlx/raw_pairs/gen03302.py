from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2025-01-01")
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)
