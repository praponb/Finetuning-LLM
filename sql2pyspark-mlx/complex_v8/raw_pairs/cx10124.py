from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_month_by_rea = Window.partitionBy("quality", F.month("read_at")).orderBy("read_at_year")
w_read_at_to_date_read_at_year = Window.partitionBy(F.to_date("read_at"), "read_at_year").orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_all_by_read_at_year = Window.orderBy("read_at_year", "id")
w_quality_read_at_year = Window.partitionBy("quality", "read_at_year")
w_read_at_year_read_at_year_te = Window.partitionBy("read_at_year", "read_at_year_tercile")
w_read_at_year_by_read_at_year = Window.partitionBy("read_at_year").orderBy(F.col("read_at_year_percent_rank").desc())
w_read_at_year_by_read_at_year_2 = Window.partitionBy("read_at_year").orderBy(F.col("read_at_year_percent_rank").desc(), "id").rowsBetween(-2, -1)
w_read_at_year_by_read_at_year_3 = Window.partitionBy("read_at_year").orderBy(F.col("read_at_year_percent_rank").desc(), "id")
w_read_at_year_by_read_at_year_4 = Window.partitionBy("read_at_year").orderBy(F.col("read_at_year_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_readings = (
    df_meter_readings
    .filter((F.col("voltage") > 215.7) | (F.col("quality") == "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

windowed = (
    base_readings
    .select(
        "id",
        "read_at_year",
        "read_at_label",
        F.round(F.percent_rank().over(w_quality_read_at_month_by_rea), 3).alias("read_at_year_percent_rank"),
        F.sum("kwh").over(w_read_at_to_date_read_at_year).alias("kwh_sum_4"),
        F.ntile(3).over(w_all_by_read_at_year).alias("read_at_year_tercile"),
        F.round(F.avg("kwh").over(w_quality_read_at_year), 2).alias("quality_read_at_year_avg_kwh"),
    )
)

labeled = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("read_at_year_tercile"), F.lit(0)).alias("read_at_year_tercile_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("read_at_label")), " ", "_")).alias("read_at_label_slug"),
    )
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.max("read_at_year_tercile").alias("overall_max_read_at_year_tercile")))
    .filter(F.col("read_at_year_tercile") < F.col("overall_max_read_at_year_tercile"))
    .drop("overall_max_read_at_year_tercile")
)

with_windows = (
    qualified
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_year_read_at_year_te).alias("rows_in_read_at_year_read_at_year_tercile"),
        F.round(F.cume_dist().over(w_read_at_year_by_read_at_year), 3).alias("read_at_year_percent_rank_cume_dist"),
        F.round(F.avg("read_at_year_tercile").over(w_read_at_year_by_read_at_year_2), 2).alias("read_at_year_tercile_avg_2_prior"),
        F.ntile(5).over(w_read_at_year_by_read_at_year_3).alias("read_at_year_percent_rank_quintile"),
        F.nth_value("read_at_year_tercile_filled", 2).over(w_read_at_year_by_read_at_year_4).alias("second_read_at_year_tercile_filled"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "read_at_label",
        "read_at_year_percent_rank",
        "kwh_sum_4",
        "read_at_year_tercile",
        "quality_read_at_year_avg_kwh",
        "read_at_year_tercile_filled",
        "read_at_label_slug",
        "rows_in_read_at_year_read_at_year_tercile",
        "read_at_year_percent_rank_cume_dist",
        "read_at_year_tercile_avg_2_prior",
        "read_at_year_percent_rank_quintile",
        "second_read_at_year_tercile_filled",
        F.least(F.col("kwh_sum_4"), F.col("read_at_year")).alias("least_kwh_sum_4_read_at_year"),
    )
)
