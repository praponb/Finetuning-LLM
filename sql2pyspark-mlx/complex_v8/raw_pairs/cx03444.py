from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), F.col("id").desc())
w_tariff_by_valid_from_full = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_by_valid_from_ties = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc())

filtered = (
    df_tariff_windows
    .filter(F.to_date(F.col("valid_from")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

annotated = (
    filtered
    .filter(F.col("valid_to") < F.lit("2025-04-05 02:00:00").cast("timestamp"))
    .withColumn("tariff_ref", F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")))
)

latest_per_key = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest_per_key
    .select(
        "*",
        F.first("valid_to").over(w_tariff_by_valid_from_full).alias("first_valid_to"),
        F.round(F.percent_rank().over(w_tariff_by_valid_from_ties), 3).alias("valid_from_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "valid_from",
        "valid_to",
        "tariff_slug",
        "first_valid_to",
        "valid_from_percent_rank",
        F.date_format(F.col("first_valid_to"), "yyyy-MM-dd HH:00").alias("first_valid_to_label"),
        F.abs(F.col("valid_from_percent_rank") - 0.3).alias("valid_from_percent_rank_distance"),
    )
    .orderBy("valid_from", "id")
    .limit(5)
)
