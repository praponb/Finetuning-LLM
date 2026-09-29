from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("status") == "REFUNDED") & F.hour("paid_at").between(7, 11) & F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"])))
    .select(
        "id",
        "status",
        "paid_at",
        F.substring("method", 1, 2).alias("method_prefix"),
    )
    .orderBy(F.col("id").desc())
)
