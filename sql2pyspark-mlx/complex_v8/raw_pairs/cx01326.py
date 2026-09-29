from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_len_site_id_by_volta = Window.partitionBy("quality_len", "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_quality_len_site_id_by_volta_2 = Window.partitionBy("quality_len", "site_id").orderBy(F.col("voltage").asc_nulls_last())
w_dedup = Window.partitionBy("kwh_tier").orderBy(F.col("prev_voltage").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 83.8, "large").when(F.col("kwh") >= 42.1, "medium").otherwise("small").alias("kwh_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_activity = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)) & (F.col("m.quality").isin("OK", "EST")), "left_anti")
)

windowed = (
    with_activity
    .withColumn("prev_voltage", F.lag("voltage").over(w_quality_len_site_id_by_volta))
    .withColumn("voltage_percent_rank", F.round(F.percent_rank().over(w_quality_len_site_id_by_volta_2), 3))
)

latest = (
    windowed
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .filter(F.col("kwh_tier").isin("small", "medium"))
    .select(
        "*",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

enriched_2 = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "kwh_tier",
        "quality_len",
        "prev_voltage",
        "voltage_percent_rank",
        "read_at_label",
        "quality_group",
        F.abs(F.col("voltage") - 222.6).alias("voltage_distance"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    enriched_2.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "voltage_percent_rank",
        "prev_voltage",
        "voltage",
    )
    .unpivot(
        ids=["id"],
        values=["voltage_percent_rank", "prev_voltage", "voltage"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
