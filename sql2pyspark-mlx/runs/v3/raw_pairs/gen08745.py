from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "status",
        "amount",
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
    )
)
