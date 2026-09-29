from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .filter(F.col("row_count") < 4)
    .select("status", "row_count", "account_type_values")
)
