from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("balance").between(36000, 69000))
    .withColumn("adjusted_balance", F.round((F.col("balance") * 0.9), 2))
    .withColumn("status_filled", F.coalesce("status", F.lit("N/A")))
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        F.col("adjusted_balance").alias("ADJUSTED_BALANCE"),
        "balance",
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["ADJUSTED_BALANCE", "balance"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
