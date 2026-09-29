from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_reading_read = Window.partitionBy("reading_quality", F.month("reading_read_at"))
w_reading_read_at_month_by_rea = Window.partitionBy(F.month("reading_read_at")).orderBy("reading_read_at", "id", "reading_id")

prepared = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
    )
)

with_children_readings = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("BAD", "OK")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

sequenced = (
    with_children_readings
    .select(
        "*",
        F.min("kwh").over(w_reading_quality_reading_read).alias("reading_quality_reading_read_at_month_min_kwh"),
        F.lead("reading_read_at").over(w_reading_read_at_month_by_rea).alias("next_reading_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_reading_quality_reading_read), 2).alias("kwh_vs_reading_quality_reading_read_at_month_avg"),
        F.round((F.unix_timestamp("reading_read_at") - F.unix_timestamp(F.lag("reading_read_at").over(w_reading_read_at_month_by_rea))) / 60, 1).alias("minutes_since_prev_reading_read_at"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["quality", "reading_quality"], ["quality"], []],
        "quality", "reading_quality",
    )
    .agg(
        F.min("reading_quality_reading_read_at_month_min_kwh").alias("min_reading_quality_reading_read_at_month_min_kw"),
        F.sum(F.when(F.col("reading_quality").isin("EST", "BAD"), 1).otherwise(0)).alias("n_reading_quality_in_est_bad"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("reading_quality").asc_nulls_last(),
    )
)
