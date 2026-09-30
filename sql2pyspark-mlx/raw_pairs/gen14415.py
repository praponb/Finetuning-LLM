from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("method_clean", F.coalesce("method", F.lit("OTHER")))
)

result = (
    cleaned
    .select("id", "status", "amount", F.col("amount_filled").alias("AMOUNT_FILLED"))
    .unpivot(
        ids=["id", "status"],
        values=["amount", "AMOUNT_FILLED"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
