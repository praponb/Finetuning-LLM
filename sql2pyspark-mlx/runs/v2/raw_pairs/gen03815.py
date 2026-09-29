from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]) & F.col("account_type").isin(["SAVINGS", "CREDIT"]))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .orderBy(F.col("avg_balance").desc())
)
