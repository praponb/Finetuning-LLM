from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_to = Window.partitionBy("tariff").orderBy(F.col("valid_to").desc(), "id")

cleaned = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-12 23:00:00").cast("timestamp")) & (F.col("valid_from") < F.lit("2025-02-16 11:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_tariff_by_valid_to), 3).alias("valid_to_percent_rank"),
        F.row_number().over(w_tariff_by_valid_to).alias("seq_no"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["tariff_or_unknown", "tariff", "seq_no"], ["tariff_or_unknown"], []],
        "tariff_or_unknown", "tariff", "seq_no",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("tariff").isin("FLAT", "TOU"), F.col("seq_no"))).alias("seq_no_tariff_in_flat_tou"),
        F.count("*").alias("row_count"),
        F.percentile_approx("valid_to_percent_rank", 0.5).alias("median_valid_to_percent_rank"),
    )
    .orderBy(
        F.col("tariff_or_unknown").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
