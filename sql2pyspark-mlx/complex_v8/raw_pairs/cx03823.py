from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_from_full = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_or_unknown = Window.partitionBy("tariff_or_unknown")
w_tariff_by_valid_from = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), "id")
w_dedup = Window.partitionBy("tariff_or_unknown").orderBy("valid_to", "id")

filtered_tariffs = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_from")).between(10, 12)) & (F.col("rate") < 7.6))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

sequenced = (
    filtered_tariffs
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_or_unknown",
        "tariff_slug",
        F.nth_value("rate", 2).over(w_tariff_by_valid_from_full).alias("second_rate"),
        F.min("rate").over(w_tariff_or_unknown).alias("tariff_or_unknown_min_rate"),
        F.count(F.lit(1)).over(w_tariff_or_unknown).alias("rows_in_tariff_or_unknown"),
        F.round(F.cume_dist().over(w_tariff_by_valid_from), 3).alias("valid_from_cume_dist"),
    )
)

latest_per_key = (
    sequenced
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .filter(F.col("tariff_slug").isNotNull())
    .select(
        "id",
        "rate",
        "tariff_or_unknown",
        "tariff_slug",
        "rows_in_tariff_or_unknown",
        "valid_from_cume_dist",
        F.to_date(F.col("valid_to")).alias("valid_to_date"),
        F.date_format(F.col("valid_to"), "yyyy-MM-dd HH:00").alias("valid_to_label"),
    )
    .orderBy(
        F.col("rows_in_tariff_or_unknown").desc(),
        F.col("valid_from_cume_dist").desc(),
    )
)
