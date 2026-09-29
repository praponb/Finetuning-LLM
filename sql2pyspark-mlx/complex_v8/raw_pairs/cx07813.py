from pyspark.sql import functions as F

valid_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_from") < F.lit("2025-02-16 11:00:00").cast("timestamp")) & (F.col("tariff") == "EV"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

derived = valid_tariffs.select("*", F.abs(F.col("rate") - 5.9).alias("rate_distance"))

result = (
    derived
    .filter(F.to_date(F.col("valid_to")) < F.lit("2025-04-05").cast("date"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate_distance",
        F.date_trunc("week", F.col("valid_from")).alias("valid_from_week"),
        F.date_format(F.col("valid_to"), "yyyy-MM").alias("valid_to_label"),
    )
    .orderBy(F.col("id").desc())
)
