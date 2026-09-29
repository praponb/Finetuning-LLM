from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-05-25"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

result = (
    base
    .select("id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_time") >= "2023-06-16")
        .select("id")
    )
)
