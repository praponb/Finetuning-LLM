from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy("start_date")
w_status_start_date_year = Window.partitionBy("status", F.year("start_date"))
w_holder_id_by_start_date_prio = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy("start_date", "id")

prepared_policys = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-03-27").cast("date"), F.lit("2025-05-02").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

ranked = (
    prepared_policys
    .select(
        "*",
        F.rank().over(w_status_holder_id_by_start_da).alias("start_date_rank"),
        F.round(F.avg("premium").over(w_status_start_date_year), 2).alias("status_start_date_year_avg_premium"),
        F.count(F.lit(1)).over(w_status_start_date_year).alias("rows_in_status_start_date_year"),
        F.round(F.avg("premium").over(w_holder_id_by_start_date_prio), 2).alias("premium_avg_1_prior"),
        F.lead("premium", 3, 0).over(w_status_holder_id_by_start_da_2).alias("next3_premium"),
        F.max("premium").over(w_status_start_date_year).alias("status_start_date_year_max_premium"),
    )
)

grouped = (
    ranked
    .filter(F.col("status") == "EXPIRED")
    .groupBy("holder_id")
    .agg(
        F.min("next3_premium").alias("min_next3_premium"),
        F.round(F.sum("status_start_date_year_max_premium") / F.nullif(F.sum("premium_avg_1_prior"), F.lit(0)), 3).alias("status_start_date_year_max_premium_to_premium_av"),
        F.round(F.sum("premium") / F.nullif(F.sum("status_start_date_year_max_premium"), F.lit(0)), 3).alias("premium_to_status_start_date_year_max_premium_ra"),
        F.percentile_approx("status_start_date_year_max_premium", 0.5).alias("median_status_start_date_year_max_premium"),
        F.round(100 * F.count(F.when(~F.col("status").isin("ACTIVE", "EXPIRED"), 1)) / F.count("*"), 1).alias("pct_status_not_in_active_expired"),
        F.count("*").alias("cnt"),
    )
)

result = (
    grouped
    .select(
        "holder_id",
        "min_next3_premium",
        "status_start_date_year_max_premium_to_premium_av",
        "premium_to_status_start_date_year_max_premium_ra",
        "median_status_start_date_year_max_premium",
        "pct_status_not_in_active_expired",
        "cnt",
        F.abs(F.col("cnt") - 6).alias("cnt_distance"),
    )
    .orderBy(
        "premium_to_status_start_date_year_max_premium_ra",
        F.col("min_next3_premium").desc(),
    )
)
