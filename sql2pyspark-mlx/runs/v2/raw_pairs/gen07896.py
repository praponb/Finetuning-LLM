from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2024-07-01") & (F.col("status").isin(["OPEN", "FROZEN"]) | (F.col("account_type") == "SAVINGS")))
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
        "status",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("status_values")
)
