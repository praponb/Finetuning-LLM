from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_year_by_valid_from = Window.partitionBy(F.year("valid_to")).orderBy(F.col("valid_from").desc_nulls_first(), "id")
w_valid_from_year_by_valid_to = Window.partitionBy(F.year("valid_from")).orderBy(F.col("valid_to").desc(), "id")
w_valid_from_year_by_valid_to_2 = Window.partitionBy(F.year("valid_from")).orderBy(F.col("valid_to").desc())
w_valid_from_year_by_valid_to_3 = Window.partitionBy(F.year("valid_from")).orderBy("valid_to")
w_valid_from_year_by_valid_to_4 = Window.partitionBy(F.year("valid_from")).orderBy(F.col("valid_to").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-09 05:00:00").cast("timestamp")) & (~F.col("tariff").isin("FLAT", "EV")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.when(F.col("rate") >= 6.1, "large").when(F.col("rate") >= 4.2, "medium").otherwise("small").alias("rate_tier"),
    )
)

metrics = (
    prepared_tariffs
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate_tier",
        F.row_number().over(w_valid_to_year_by_valid_from).alias("rn"),
        F.lead("valid_to").over(w_valid_from_year_by_valid_to).alias("next_valid_to"),
        F.round(F.percent_rank().over(w_valid_from_year_by_valid_to_2), 3).alias("valid_to_percent_rank"),
        F.round(F.cume_dist().over(w_valid_from_year_by_valid_to_2), 3).alias("valid_to_cume_dist"),
        F.lead("valid_from").over(w_valid_to_year_by_valid_from).alias("next_valid_from"),
        F.round(F.cume_dist().over(w_valid_from_year_by_valid_to_3), 3).alias("valid_to_cume_dist_2"),
        F.last("valid_to").over(w_valid_from_year_by_valid_to_4).alias("last_valid_to"),
        F.lag("valid_from").over(w_valid_to_year_by_valid_from).alias("prev_valid_from"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate_tier",
        "next_valid_to",
        "valid_to_percent_rank",
        "valid_to_cume_dist",
        "next_valid_from",
        "valid_to_cume_dist_2",
        F.to_date(F.col("prev_valid_from")).alias("prev_valid_from_date"),
        F.when(F.col("rn") >= 8, "A").when(F.col("rn") >= 3, "B").otherwise("C").alias("rn_tier"),
    )
)

result = (
    annotated
    .filter(F.col("valid_to_cume_dist_2") <= 0.6)
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate_tier",
        "next_valid_to",
        "valid_to_percent_rank",
        "valid_to_cume_dist",
        "next_valid_from",
        "prev_valid_from_date",
        F.round(F.col("valid_to_percent_rank") / F.nullif(F.col("valid_to_cume_dist"), F.lit(0)), 3).alias("valid_to_percent_rank_per_valid_to_cume_dist"),
    )
    .orderBy("valid_from")
)
