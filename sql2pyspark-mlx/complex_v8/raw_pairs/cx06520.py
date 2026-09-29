from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

result = (
    sessionized
    .groupingSets(
        [["session_id", "is_new_session", "site_id"], ["session_id"], ["is_new_session"]],
        "session_id", "is_new_session", "site_id",
    )
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("session_id").asc_nulls_last(),
        F.col("is_new_session").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
