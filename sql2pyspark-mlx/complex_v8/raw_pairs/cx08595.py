from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_valid_from_date_month = Window.partitionBy("tariff", F.date_trunc("month", "valid_from_date")).orderBy(F.col("valid_to").desc(), "id")
w_tariff_valid_from_date_month_2 = Window.partitionBy("tariff", F.date_trunc("month", "valid_from_date")).orderBy(F.col("valid_to").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_valid_from_date_month_3 = Window.partitionBy("tariff", F.date_trunc("month", "valid_from_date")).orderBy(F.col("valid_to").desc())
w_valid_to_rank_valid_to_decil = Window.partitionBy("valid_to_rank", "valid_to_decile")
w_valid_to_decile_valid_from_d = Window.partitionBy("valid_to_decile", F.year("valid_from_date_week")).orderBy("valid_to_decile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_from_month_by_valid_to = Window.partitionBy(F.date_trunc("month", "valid_from")).orderBy(F.col("valid_to_decile").desc(), "id").rowsBetween(-2, -1)
w_valid_from_month_by_valid_to_2 = Window.partitionBy(F.date_trunc("month", "valid_from")).orderBy(F.col("valid_to_decile").desc(), "id")
w_last_known_valid_to_rank_pre = Window.partitionBy("last_known_valid_to_rank", "prev3_valid_to_rank").orderBy(F.col("last_known_valid_to_rank").desc(), "id")
w_last_known_valid_to_rank_pre_2 = Window.partitionBy("last_known_valid_to_rank", "prev3_valid_to_rank").orderBy(F.col("last_known_valid_to_rank").desc(), "id").rowsBetween(-3, -1)
w_valid_from_date_week_to_date = Window.partitionBy(F.to_date("valid_from_date_week"), "prev3_valid_to_rank")

base_tariffs = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
    )
)

sequenced = (
    base_tariffs
    .select(
        "*",
        F.ntile(10).over(w_tariff_valid_from_date_month).alias("valid_to_decile"),
        F.first("valid_to", ignorenulls=True).over(w_tariff_valid_from_date_month_2).alias("first_known_valid_to"),
        F.rank().over(w_tariff_valid_from_date_month_3).alias("valid_to_rank"),
        F.round((F.unix_timestamp("valid_to") - F.unix_timestamp(F.lag("valid_to").over(w_tariff_valid_from_date_month))) / 60, 1).alias("minutes_since_prev_valid_to"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "valid_to_decile",
        "valid_to_rank",
        "minutes_since_prev_valid_to",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.abs(F.col("valid_to_rank") - 7).alias("valid_to_rank_distance"),
        F.date_trunc("week", F.col("valid_from_date")).alias("valid_from_date_week"),
    )
)

tagged = scored.select("*", F.date_trunc("week", F.col("valid_to")).alias("valid_to_week"))

with_trends = (
    tagged
    .select(
        "id",
        "valid_from",
        "valid_from_date_week",
        "valid_to_week",
        F.round(F.col("valid_to_rank_distance") - F.avg("valid_to_rank_distance").over(w_valid_to_rank_valid_to_decil), 2).alias("valid_to_rank_distance_vs_valid_to_rank_valid_to"),
        F.last("valid_to_rank", ignorenulls=True).over(w_valid_to_decile_valid_from_d).alias("last_known_valid_to_rank"),
        F.sum("minutes_since_prev_valid_to").over(w_valid_from_month_by_valid_to).alias("minutes_since_prev_valid_to_sum_2_prior"),
        F.lag("valid_to_rank", 3, 0).over(w_valid_from_month_by_valid_to_2).alias("prev3_valid_to_rank"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.rank().over(w_last_known_valid_to_rank_pre).alias("last_known_valid_to_rank_rank"),
        F.round(F.avg("prev3_valid_to_rank").over(w_last_known_valid_to_rank_pre_2), 2).alias("prev3_valid_to_rank_avg_3_prior"),
        F.round(F.col("last_known_valid_to_rank") / F.nullif(F.sum("last_known_valid_to_rank").over(w_valid_from_date_week_to_date), F.lit(0)), 4).alias("last_known_valid_to_rank_share_of_valid_from_dat"),
        F.round(F.col("last_known_valid_to_rank") - F.avg("last_known_valid_to_rank").over(w_valid_from_date_week_to_date), 2).alias("last_known_valid_to_rank_vs_valid_from_date_week"),
        F.max("last_known_valid_to_rank").over(w_valid_from_date_week_to_date).alias("valid_from_date_week_to_date_prev3_valid_to_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "minutes_since_prev_valid_to_sum_2_prior",
        "prev3_valid_to_rank_avg_3_prior",
        "last_known_valid_to_rank_vs_valid_from_date_week",
    )
    .unpivot(
        ids=["id"],
        values=["minutes_since_prev_valid_to_sum_2_prior", "prev3_valid_to_rank_avg_3_prior", "last_known_valid_to_rank_vs_valid_from_date_week"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
