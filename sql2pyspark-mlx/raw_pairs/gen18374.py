from pyspark.sql import functions as F

base = df_accounts.filter(F.col("opened_date") >= "2024-01-01")

summary = (
    base
    .groupBy("account_type")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    summary
    .filter(F.col("unique_statuss") >= 8)
    .select("account_type", "account_type_values")
    .orderBy("account_type", "account_type_values")
    .limit(3)
)
