from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("rate_tier").orderBy(F.col("valid_to").desc(), F.col("id").desc())

recent_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_to") >= F.lit("2025-02-18 04:00:00").cast("timestamp")) & (F.col("tariff") != "EV"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("rate") >= 5.9, "large").when(F.col("rate") >= 4.2, "medium").otherwise("small").alias("rate_tier"),
    )
)

deduped = (
    recent_tariffs
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    deduped
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 3600, 1).alias("hours_valid_from_to_valid_to"),
        F.hour(F.col("valid_from")).alias("valid_from_hour"),
        F.substring(F.trim(F.col("tariff")), 1, 3).alias("tariff_prefix"),
    )
)

result = (
    labeled
    .groupBy("valid_to", "valid_from")
    .pivot("tariff", ["FLAT", "EV", "TOU"])
    .agg(F.min("rate"))
    .orderBy("valid_to", "valid_from")
)
