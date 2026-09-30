from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_to_date = Window.partitionBy(F.to_date("valid_to"))
w_all_by_rate_distance_distanc = Window.orderBy("rate_distance_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tariff_windows
    .filter((F.col("valid_to") >= F.lit("2025-02-16 05:00:00").cast("timestamp")) | (F.col("rate") > 4.2))
)

annotated = (
    base
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.abs(F.col("rate") - 4.2).alias("rate_distance"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

tagged = (
    annotated
    .select(
        "*",
        (F.col("rate") - F.col("rate_distance")).alias("rate_minus_rate_distance"),
        F.coalesce(F.col("rate_distance"), F.lit(0.0)).alias("rate_distance_filled"),
        F.abs(F.col("rate_distance") - 5.2).alias("rate_distance_distance"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "valid_from",
        "tariff_ref",
        "rate_minus_rate_distance",
        "rate_distance_filled",
        "rate_distance_distance",
        F.sum("rate_minus_rate_distance").over(w_valid_to_to_date).alias("valid_to_to_date_sum_rate_minus_rate_distance"),
        F.first("valid_from", ignorenulls=True).over(w_all_by_rate_distance_distanc).alias("first_known_valid_from"),
    )
)

result = (
    metrics
    .select(
        "id",
        "valid_from",
        "tariff_ref",
        "rate_minus_rate_distance",
        "rate_distance_filled",
        "rate_distance_distance",
        "valid_to_to_date_sum_rate_minus_rate_distance",
        "first_known_valid_from",
    )
    .orderBy(F.col("tariff_ref").desc_nulls_first(), "id")
    .limit(10)
)
