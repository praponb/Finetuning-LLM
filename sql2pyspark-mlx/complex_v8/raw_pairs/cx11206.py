from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_valid_from = Window.orderBy("valid_from", "id")
w_tariff_by_valid_from = Window.partitionBy("tariff").orderBy("valid_from", "id")
w_all_by_valid_from_full = Window.orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-02-18 04:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

derived = prepared_tariffs.withColumn("tariff_prefix", F.substring(F.trim(F.col("tariff")), 1, 3))

with_windows = (
    derived
    .select(
        "id",
        "tariff",
        "valid_to",
        "tariff_prefix",
        F.lead("valid_to").over(w_all_by_valid_from).alias("next_valid_to"),
        F.row_number().over(w_tariff_by_valid_from).alias("seq_no"),
        F.last("valid_to").over(w_all_by_valid_from_full).alias("last_valid_to"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown_2"),
    )
)

rollup_base = (
    scored
    .groupBy("seq_no", "tariff", "tariff_or_unknown")
    .agg(
        F.sum(F.when(F.col("tariff").isin("TOU", "FLAT"), 1).otherwise(0)).alias("n_tariff_in_tou_flat"),
        F.sum(F.when(F.col("tariff") != "FLAT", F.col("seq_no")).otherwise(0)).alias("seq_no_tariff_flat"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.count("*").alias("n_rows"),
    )
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("n_tariff_in_tou_flat").alias("overall_avg_n_tariff_in_tou_flat")))
    .filter(F.col("n_tariff_in_tou_flat") > F.col("overall_avg_n_tariff_in_tou_flat"))
    .drop("overall_avg_n_tariff_in_tou_flat")
)

result = (
    qualified
    .select(
        "seq_no",
        "tariff",
        "tariff_or_unknown",
        "n_tariff_in_tou_flat",
        "seq_no_tariff_flat",
        "tariff_list",
        "n_rows",
        F.initcap(F.trim(F.col("tariff_or_unknown"))).alias("tariff_or_unknown_clean"),
    )
    .orderBy(F.col("tariff").desc_nulls_first())
)
