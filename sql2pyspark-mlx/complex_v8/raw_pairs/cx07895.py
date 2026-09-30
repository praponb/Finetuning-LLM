from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_site_id_by_read_a = Window.partitionBy("session_no", "site_id").orderBy(F.col("read_at").desc())
w_session_no_site_id_by_read_a_2 = Window.partitionBy("session_no", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("kwh") > 20.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_site_id_running))

with_trends = (
    sessionized
    .withColumn("read_at_dense_rank", F.dense_rank().over(w_session_no_site_id_by_read_a))
    .withColumn("new_session_flag_max_2", F.max("new_session_flag").over(w_session_no_site_id_by_read_a_2))
)

result = (
    with_trends
    .select(
        "id",
        "kwh",
        "voltage",
        "voltage_minus_kwh",
        "prev_read_at",
        "new_session_flag",
        "session_no",
        "read_at_dense_rank",
        "new_session_flag_max_2",
    )
    .orderBy("voltage")
)
