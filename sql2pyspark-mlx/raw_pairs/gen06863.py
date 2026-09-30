from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)
