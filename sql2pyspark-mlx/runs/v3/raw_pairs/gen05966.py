from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED"]))
    .select(
        "opened_date",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Priority").otherwise("Other").alias("status_group"),
    )
    .orderBy("status_group")
)
