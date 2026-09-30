from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.when(F.col("status").isin(["OPEN", "CLOSED"]), "core").otherwise("other").alias("status_group"))
    .pivot("status", ["CLOSED", "OPEN", "FROZEN"])
    .agg(F.avg("balance"))
)
