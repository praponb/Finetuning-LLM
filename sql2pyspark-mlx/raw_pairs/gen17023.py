from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-05-24").cast("date")) & F.col("status").isNotNull())
    .withColumn("status_length", F.length("status"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("OTHER")))
)

result = (
    filtered
    .select("id", "status")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") >= "2024-12-16")
        .select("id", "status")
    )
)
