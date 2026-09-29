from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

annotated = (
    sessionized
    .filter(F.col("voltage") < 231.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "kwh_per_voltage",
        "prev_read_at",
        "session_id",
        F.greatest(F.col("voltage"), F.col("session_id")).alias("greatest_voltage_session_id"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

totals = (
    annotated
    .groupBy("site_id", F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.sum("greatest_voltage_session_id"), F.lit(0)), 3).alias("session_id_to_greatest_voltage_session_id_ratio"),
        F.min("session_id").alias("min_session_id"),
        F.min("kwh_per_voltage").alias("min_kwh_per_voltage"),
        F.count(F.when(F.col("quality") == "EST", 1)).alias("n_quality_est"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .select(
        "site_id",
        "read_at_month",
        F.col("min_kwh_per_voltage").alias("m_min_kwh_per_voltage"),
        F.col("session_id_to_greatest_voltage_session_id_ratio").alias("session_id_to_greatest_voltage_session_id_ratio_"),
    )
    .unpivot(
        ids=["site_id", "read_at_month"],
        values=["m_min_kwh_per_voltage", "session_id_to_greatest_voltage_session_id_ratio_"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
