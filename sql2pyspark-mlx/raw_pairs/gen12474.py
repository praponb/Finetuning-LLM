from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-04-23").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round("amount", 2).alias("amount_rounded"),
    )
)

result = (
    valid
    .select("id", "status")
    .intersectAll(
        df_payments
        .filter(F.col("paid_at") < "2023-08-01")
        .select("id", "status")
    )
)
