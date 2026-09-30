from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("tariff").orderBy(F.col("rate").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("tariff").orderBy(F.col("valid_to_hour").desc(), F.col("id").desc())

cleaned_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

latest_per_key = (
    cleaned_tariffs
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_per_key_2 = (
    latest_per_key
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    latest_per_key_2
    .filter(F.col("valid_to") < F.lit("2025-03-09 05:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.when(F.col("rate") >= 6.1, "HIGH").when(F.col("rate") >= 3.7, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
    )
)

annotated_2 = (
    annotated
    .filter(F.col("tariff_ref").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.date_trunc("hour", F.col("valid_to")).alias("valid_to_hour"),
    )
)

latest = (
    annotated_2
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupingSets(
        [["rate_tier", "tariff"], ["rate_tier"], ["tariff"], []],
        "rate_tier", "tariff",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("rate"), 2).alias("stddev_rate"),
        F.round(F.sum("rate"), 2).alias("total_rate"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("rate_tier"), F.lit("ALL")).alias("rate_tier"),
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "grouping_level",
        "stddev_rate",
        "total_rate",
        "cnt",
    )
    .orderBy(F.col("rate_tier").asc_nulls_last(), F.col("tariff").asc_nulls_last())
)
