from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_year = Window.partitionBy("site_id").orderBy(F.col("read_at_year").desc(), "id")
w_read_at_month_read_at_year = Window.partitionBy(F.date_trunc("month", "read_at"), "read_at_year")
w_read_at_year_quality_by_read = Window.partitionBy("read_at_year", "quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage") <= 233.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

metrics = (
    cleaned_readings
    .select(
        "*",
        F.round(F.percent_rank().over(w_site_id_by_read_at_year), 3).alias("read_at_year_percent_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month_read_at_year), 2).alias("kwh_vs_read_at_month_read_at_year_avg"),
        F.round(F.col("read_at_year") - F.avg("read_at_year").over(w_read_at_month_read_at_year), 2).alias("read_at_year_vs_read_at_month_read_at_year_avg"),
        F.round(F.col("read_at_year") / F.nullif(F.sum("read_at_year").over(w_read_at_month_read_at_year), F.lit(0)), 4).alias("read_at_year_share_of_read_at_month_read_at_year"),
        F.sum("kwh").over(w_read_at_month_read_at_year).alias("read_at_month_read_at_year_sum_kwh"),
        F.last("read_at").over(w_read_at_year_quality_by_read).alias("last_read_at"),
        F.size(F.collect_set("site_id").over(w_read_at_month_read_at_year)).alias("distinct_site_id_in_read_at_month_read_at_year"),
    )
)

annotated = (
    metrics
    .filter(F.col("last_read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .withColumn("quality_len", F.length(F.trim(F.col("quality"))))
)

stats = (
    annotated
    .groupBy("quality", "site_id")
    .agg(
        F.round(F.sum("read_at_year_percent_rank"), 2).alias("total_read_at_year_percent_rank"),
        F.max("last_read_at").alias("last_last_read_at"),
        F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)).alias("n_quality_not_in_bad_est"),
    )
)

result = (
    stats
    .filter(F.col("n_quality_not_in_bad_est") <= 8)
    .select(
        "quality",
        "site_id",
        "total_read_at_year_percent_rank",
        "last_last_read_at",
        "n_quality_not_in_bad_est",
        F.concat(F.col("quality"), F.lit("#"), F.col("n_quality_not_in_bad_est").cast("string")).alias("quality_ref"),
        F.dayofweek(F.col("last_last_read_at")).alias("last_last_read_at_dayofweek"),
    )
    .orderBy(
        F.col("n_quality_not_in_bad_est").desc_nulls_first(),
        "total_read_at_year_percent_rank",
        "quality",
        "site_id",
    )
    .limit(5)
)
