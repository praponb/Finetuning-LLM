from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_year_rate_tier = Window.partitionBy(F.year("valid_to"), "rate_tier")
w_valid_to_month_by_rate = Window.partitionBy(F.month("valid_to")).orderBy("rate", "id")

filtered_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_to") < F.lit("2025-03-27 13:00:00").cast("timestamp"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("rate") >= 6.1, "large").when(F.col("rate") >= 4.2, "medium").otherwise("small").alias("rate_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 60, 1).alias("minutes_valid_to_to_valid_from"),
    )
)

sequenced = (
    filtered_tariffs
    .select(
        "id",
        "valid_from",
        "rate",
        "minutes_valid_to_to_valid_from",
        F.count(F.lit(1)).over(w_valid_to_year_rate_tier).alias("rows_in_valid_to_year_rate_tier"),
        F.rank().over(w_valid_to_month_by_rate).alias("rate_rank"),
        F.round(F.col("days_since_valid_from") / F.nullif(F.sum("days_since_valid_from").over(w_valid_to_year_rate_tier), F.lit(0)), 4).alias("days_since_valid_from_share_of_valid_to_year_rat"),
    )
)

tagged = (
    sequenced
    .select(
        "id",
        "valid_from",
        "rate",
        "days_since_valid_from_share_of_valid_to_year_rat",
        F.abs(F.col("rate_rank") - 7).alias("rate_rank_distance"),
        (F.col("rate_rank") - F.col("rate")).alias("rate_rank_minus_rate"),
    )
)

qualified = (
    tagged
    .crossJoin(tagged.agg(F.avg("rate_rank_distance").alias("overall_avg_rate_rank_distance")))
    .filter(F.col("rate_rank_distance") > F.col("overall_avg_rate_rank_distance"))
    .drop("overall_avg_rate_rank_distance")
)

result = (
    qualified
    .select("id", "days_since_valid_from_share_of_valid_to_year_rat", "rate")
    .unpivot(
        ids=["id"],
        values=["days_since_valid_from_share_of_valid_to_year_rat", "rate"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
