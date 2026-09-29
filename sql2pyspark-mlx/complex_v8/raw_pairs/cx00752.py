from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff").orderBy(F.col("valid_from_hour").desc(), F.col("id").desc())

filtered_tariffs = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(8, 10))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.upper(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.date_trunc("hour", F.col("valid_from")).alias("valid_from_hour"),
    )
)

latest_per_key = (
    filtered_tariffs
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "valid_from",
        "valid_to",
        "valid_from_hour",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff_clean")), " ", "_")).alias("tariff_clean_slug"),
    )
    .orderBy("valid_from_hour", F.col("tariff_clean_slug").desc())
)
