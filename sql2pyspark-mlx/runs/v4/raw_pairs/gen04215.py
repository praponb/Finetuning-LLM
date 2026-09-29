from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "cash"]))
    .select(
        "id",
        "status",
        "paid_at",
        F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"),
    )
)
