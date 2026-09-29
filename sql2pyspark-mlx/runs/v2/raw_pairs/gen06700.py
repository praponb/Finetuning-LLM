from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("balance") > 69000)
    .withColumn("balance_tier", F.when(F.col("balance") >= 81000, "High").when(F.col("balance") >= 10000, "Medium").otherwise("Low"))
)

opened_year_summary = (
    recent_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "status")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    opened_year_summary
    .select("opened_year", "status", "account_type_values")
    .orderBy(F.col("opened_year").desc(), "status")
    .limit(50)
)
