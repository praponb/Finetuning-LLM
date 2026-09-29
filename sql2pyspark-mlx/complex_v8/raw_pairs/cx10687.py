from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("end_date") >= F.lit("2025-12-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        (F.col("premium") + F.col("premium_adj")).alias("premium_plus_premium_adj"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

result = (
    scored
    .groupBy("start_date")
    .pivot("product", ["HOME", "TRAVEL"])
    .agg(F.sum("premium_plus_premium_adj"))
)
