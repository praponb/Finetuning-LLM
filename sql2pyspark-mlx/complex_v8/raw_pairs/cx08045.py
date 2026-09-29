from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_quality_by_r = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id")
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc(), "id")
w_quality_by_prev_voltage = Window.partitionBy("quality").orderBy(F.col("prev_voltage").asc_nulls_last(), "id")
w_quality_by_prev_voltage_ties = Window.partitionBy("quality").orderBy(F.col("prev_voltage").asc_nulls_last())
w_quality_by_prev_voltage_runn = Window.partitionBy("quality").orderBy(F.col("prev_voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

sequenced = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        F.lag("voltage").over(w_read_at_to_date_quality_by_r).alias("prev_voltage"),
        F.lead("voltage").over(w_quality_site_id_by_voltage).alias("next_voltage"),
        F.dense_rank().over(w_read_at_to_date_quality_by_r).alias("read_at_dense_rank"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.lead("read_at", 3).over(w_quality_by_prev_voltage).alias("next3_read_at"),
        F.round(F.cume_dist().over(w_quality_by_prev_voltage_ties), 3).alias("prev_voltage_cume_dist"),
        F.lead("read_at").over(w_quality_by_prev_voltage).alias("next_read_at"),
        F.count(F.lit(1)).over(w_quality_by_prev_voltage_runn).alias("running_count"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("kwh").alias("min_reading_kwh"),
        F.sum("kwh").alias("total_reading_kwh"),
    )
)

profiled = (
    windowed.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_kwh", "m.total_reading_kwh")
)

result = (
    profiled
    .rollup("quality", "read_at_dense_rank")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "read_at_dense_rank",
        "n_quality_bad",
        "distinct_ids",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("read_at_dense_rank").asc_nulls_last(),
    )
)
