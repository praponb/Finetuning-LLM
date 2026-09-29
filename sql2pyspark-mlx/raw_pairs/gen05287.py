from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED"]))
    .groupBy("customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("avg_balance") < 69000)
    .drop("avg_balance")
)
