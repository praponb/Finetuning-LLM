from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("avg_balance") >= 57000)
    .orderBy(F.col("avg_balance").desc(), "status_values")
)
