from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > "2024-02-01") & F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
)

summary = (
    base
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
)

result = (
    summary
    .select("customer_id", "account_type_values", "frozen_count")
    .orderBy(F.col("account_type_values").desc(), F.col("customer_id").desc())
)
