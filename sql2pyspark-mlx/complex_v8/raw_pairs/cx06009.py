from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_kwh_12h = Window.orderBy("prior_kwh_12h", "id")
w_read_at_day_year = Window.partitionBy(F.year("read_at_day"))
w_all_by_prior_kwh_12h_full = Window.orderBy("prior_kwh_12h", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.kwh").alias("prior_kwh_12h"),
    )
)

annotated = (
    joined_ranges
    .filter(F.col("prior_kwh_12h") < 480.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_12h",
        "prior_kwh_12h",
        F.least(F.col("prior_rows_12h"), F.col("prior_kwh_12h")).alias("least_prior_rows_12h_prior_kwh_12h"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_12h",
        "prior_kwh_12h",
        "read_at_day",
        F.lead("prior_rows_12h").over(w_all_by_prior_kwh_12h).alias("next_prior_rows_12h"),
        F.row_number().over(w_all_by_prior_kwh_12h).alias("rn"),
        F.sum("prior_rows_12h").over(w_read_at_day_year).alias("read_at_day_year_sum_prior_rows_12h"),
        F.first("prior_rows_12h").over(w_all_by_prior_kwh_12h_full).alias("first_prior_rows_12h"),
        F.min("prior_kwh_12h").over(w_read_at_day_year).alias("read_at_day_year_min_prior_kwh_12h"),
        F.ntile(3).over(w_all_by_prior_kwh_12h).alias("prior_kwh_12h_tercile"),
    )
)

result = (
    ranked
    .cube("site_id", "prior_rows_12h", "rn")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("prior_rows_12h").asc_nulls_last(),
        F.col("rn").asc_nulls_last(),
    )
)
