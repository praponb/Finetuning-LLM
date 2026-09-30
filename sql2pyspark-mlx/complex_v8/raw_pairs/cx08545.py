from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev_read_at_running = Window.orderBy(F.col("prev_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_2 = Window.partitionBy("site_id")
w_all_by_prev_read_at_prior_1 = Window.orderBy(F.col("prev_read_at").desc(), "id").rowsBetween(-1, -1)

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

ranked = (
    sessionized
    .select(
        "*",
        F.first("session_id", ignorenulls=True).over(w_all_by_prev_read_at_running).alias("first_known_session_id"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_2), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.sum("session_id").over(w_all_by_prev_read_at_prior_1).alias("session_id_sum_1_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "session_id",
        "first_known_session_id",
        "kwh_share_of_site_id",
        "session_id_sum_1_prior",
    )
    .orderBy(F.col("quality").desc())
)
