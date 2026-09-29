from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_read_at_month_quality = Window.partitionBy(F.month("first_read_at"), "quality")
w_n_rows_by_n_rows = Window.partitionBy("n_rows").orderBy("n_rows", "quality", "read_at_ym")

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

grouped = (
    scoped
    .groupBy("quality", F.date_format("read_at", "yyyy-MM").alias("read_at_ym"))
    .agg(
        F.count("*").alias("n_rows"),
        F.min("read_at").alias("first_read_at"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

metrics = (
    grouped
    .withColumn("total_voltage_vs_first_read_at_month_quality_avg", F.round(F.col("total_voltage") - F.avg("total_voltage").over(w_first_read_at_month_quality), 2))
    .withColumn("rn", F.row_number().over(w_n_rows_by_n_rows))
)

result = (
    metrics
    .select(
        "quality",
        "read_at_ym",
        "n_rows",
        "total_voltage",
        "quality_list",
        "total_voltage_vs_first_read_at_month_quality_avg",
        "rn",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.abs(F.col("rn") - 5).alias("rn_distance"),
    )
    .orderBy(F.col("read_at_ym").desc())
)
