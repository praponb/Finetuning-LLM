from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy("kwh")

cleaned = (
    df_meter_readings
    .filter((F.col("voltage").between(220.2, 236.8)) & (F.col("read_at") >= F.lit("2025-01-13 15:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_by_kwh)).alias("voltage_change"),
        F.dense_rank().over(w_quality_by_kwh_ties).alias("kwh_dense_rank"),
        F.rank().over(w_quality_by_kwh_ties).alias("kwh_rank"),
    )
)

filtered = (
    with_trends.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.lower(F.col("si.site_name")).like("%no%")) & (F.col("si.installed_kw") < 373.3), "left_semi")
)

derived = (
    filtered
    .filter(~F.col("quality").isin("BAD", "EST"))
    .withColumn("read_at_date", F.to_date(F.col("read_at")))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
        "m.first_reading_read_at",
    )
)

totals = (
    summarized_rows
    .filter(F.col("voltage_change") >= -49.0)
    .groupBy(
        "reading_count_2",
        F.date_trunc("month", "read_at_date").alias("read_at_date_month"),
        F.when(F.col("site_id_sum_kwh") >= 720.0, "large").when(F.col("site_id_sum_kwh") >= 240.0, "medium").otherwise("small").alias("site_id_sum_kwh_tier"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage_change"), F.lit(0)), 3).alias("kwh_to_voltage_change_ratio"),
        F.round(F.sum("voltage") / F.nullif(F.sum("reading_count"), F.lit(0)), 3).alias("voltage_to_reading_count_ratio"),
    )
)

result = (
    totals
    .select(
        "reading_count_2",
        "read_at_date_month",
        "site_id_sum_kwh_tier",
        "n_rows",
        "voltage_to_reading_count_ratio",
    )
    .orderBy(F.col("read_at_date_month").asc(), "site_id_sum_kwh_tier")
)
