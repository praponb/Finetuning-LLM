from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .select(
        "id",
        "amount",
        "is_flagged",
        F.coalesce("merchant", F.lit("unassigned")).alias("merchant_filled"),
        F.round("amount", 0).alias("amount_rounded"),
    )
)
