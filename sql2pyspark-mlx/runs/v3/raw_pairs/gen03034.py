from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "status",
        F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.length("status").alias("status_length"),
    )
    .orderBy(F.col("status_length").desc())
)
