from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("balance").between(28000, 63000) | (F.col("status") != "FROZEN"))
)

summary = (
    filtered_accounts
    .groupBy(
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    summary
    .filter(F.col("total_balance") >= 206000)
    .select("status_group", "status_values", "total_balance")
    .orderBy("status_values")
)
