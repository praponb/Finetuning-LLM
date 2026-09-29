from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]) | (F.col("balance") > 18000))
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    agg
    .orderBy("status_values", "account_type")
    .limit(20)
)
