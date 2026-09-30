from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") > 196000)
)
