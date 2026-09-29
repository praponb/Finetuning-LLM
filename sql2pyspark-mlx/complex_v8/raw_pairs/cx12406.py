from pyspark.sql import functions as F

base = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(7, 9))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

filtered = (
    base
    .crossJoin(base.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

result = (
    filtered
    .filter(F.hour(F.col("valid_from")).between(6, 9))
    .select(
        "id",
        F.when(F.col("rate") >= 5.7, "HIGH").when(F.col("rate") >= 4.2, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.date_format(F.col("valid_to"), "yyyy-MM-dd HH:00").alias("valid_to_label"),
    )
)
