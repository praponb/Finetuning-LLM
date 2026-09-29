from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("account_id") == 4)
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.floor("amount").alias("amount_whole"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "account_id",
        F.col("amount_filled").alias("AMOUNT_FILLED_VAL"),
        F.col("amount").alias("AMOUNT"),
    )
    .unpivot(
        ids=["id", "account_id"],
        values=["AMOUNT_FILLED_VAL", "AMOUNT"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
