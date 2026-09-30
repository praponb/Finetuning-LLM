from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_or_unknown_tariff_by = Window.partitionBy("tariff_or_unknown", "tariff").orderBy(F.col("rate").desc(), "id")
w_tariff_tariff_or_unknown = Window.partitionBy("tariff", "tariff_or_unknown")
w_tariff_or_unknown_tariff = Window.partitionBy("tariff_or_unknown", "tariff")
w_all_by_rate_distance = Window.orderBy(F.col("rate_distance").desc_nulls_first(), "id")
w_tariff_tariff_or_unknown_by = Window.partitionBy("tariff", "tariff_or_unknown").orderBy(F.col("valid_to").desc(), "id")

prepared = (
    df_tariff_windows
    .filter((F.col("tariff") != "TOU") & (F.to_date(F.col("valid_from")) < F.lit("2025-03-09").cast("date")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.abs(F.col("rate") - 5.7).alias("rate_distance"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.row_number().over(w_tariff_or_unknown_tariff_by).alias("rn"),
        F.round(F.avg("rate_distance").over(w_tariff_tariff_or_unknown), 2).alias("tariff_tariff_or_unknown_avg_rate_distance"),
        F.min("rate").over(w_tariff_or_unknown_tariff).alias("tariff_or_unknown_tariff_min_rate"),
        F.count(F.lit(1)).over(w_tariff_tariff_or_unknown).alias("rows_in_tariff_tariff_or_unknown"),
        F.lead("rate").over(w_all_by_rate_distance).alias("next_rate"),
        (F.col("rate_distance") - F.lag("rate_distance").over(w_tariff_tariff_or_unknown_by)).alias("rate_distance_change"),
        F.count(F.lit(1)).over(w_tariff_or_unknown_tariff).alias("rows_in_tariff_or_unknown_tariff"),
    )
)

leaders = (
    windowed
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    leaders
    .select(
        "id",
        "tariff",
        "rate",
        "tariff_or_unknown",
        "tariff_or_unknown_tariff_min_rate",
        "next_rate",
        "rate_distance_change",
        "rows_in_tariff_or_unknown_tariff",
        F.when(F.col("next_rate") >= 5.7, "HIGH").when(F.col("next_rate") >= 3.0, "MEDIUM").otherwise("LOW").alias("next_rate_tier"),
    )
    .orderBy(F.col("tariff_or_unknown").desc())
)
