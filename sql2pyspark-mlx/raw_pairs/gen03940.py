from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Shell")))
    .select(
        "*",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "account_id",
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("amount_rounded").alias("AMOUNT_ROUNDED"),
    )
    .unpivot(
        ids=["id", "account_id"],
        values=["AMOUNT_VAL", "AMOUNT_ROUNDED"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
