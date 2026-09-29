from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_is_new_sessi = Window.partitionBy(F.to_date("read_at"), "is_new_session")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_quality_running))

with_windows = (
    sessionized
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "voltage_bucket",
        "prev_read_at",
        "is_new_session",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_to_date_is_new_sessi), F.lit(0)), 4).alias("voltage_share_of_read_at_to_date_is_new_session"),
        F.size(F.collect_set("quality").over(w_read_at_to_date_is_new_sessi)).alias("distinct_quality_in_read_at_to_date_is_new_sessi"),
    )
)

scored = (
    with_windows
    .filter(F.col("is_new_session") < 1)
    .withColumn("voltage_bucket_per_voltage", F.round(F.col("voltage_bucket") / F.nullif(F.col("voltage"), F.lit(0)), 3))
)

result = (
    scored
    .groupBy("prev_read_at")
    .pivot("quality", ["EST", "OK"])
    .agg(F.count("voltage_bucket"))
)
