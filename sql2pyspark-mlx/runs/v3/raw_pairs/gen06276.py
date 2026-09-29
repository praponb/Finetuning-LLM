from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"]))
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("avg_balance") >= 62)
    .orderBy("status_values")
)
