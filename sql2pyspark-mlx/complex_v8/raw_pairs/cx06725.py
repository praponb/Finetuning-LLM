from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_hour_ties = Window.partitionBy("site_id").orderBy(F.col("read_at_hour").desc())
w_site_id = Window.partitionBy("site_id")
w_read_at_year_read_at_hour_mo = Window.partitionBy(F.year("read_at"), F.date_trunc("month", "read_at_hour")).orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
    )
)

with_trends = (
    prepared_readings
    .select(
        "*",
        F.round(F.percent_rank().over(w_site_id_by_read_at_hour_ties), 3).alias("read_at_hour_percent_rank"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.count(F.lit(1)).over(w_read_at_year_read_at_hour_mo).alias("running_count"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
    )
)

tagged = (
    with_trends
    .filter(F.col("site_id") >= 2)
    .select(
        "*",
        F.when(F.col("site_id_avg_voltage") >= 234.5, "A").when(F.col("site_id_avg_voltage") >= 222.2, "B").otherwise("C").alias("site_id_avg_voltage_tier"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at_hour")).alias("days_since_read_at_hour"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 222.3) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        F.col("distinct_quality_in_site_id").alias("distinct_quality_in_site_id_v"),
        F.col("days_since_read_at_hour").alias("m_days_since_read_at_hour"),
    )
    .unpivot(
        ids=["id"],
        values=["distinct_quality_in_site_id_v", "m_days_since_read_at_hour"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("id", "field")
)
