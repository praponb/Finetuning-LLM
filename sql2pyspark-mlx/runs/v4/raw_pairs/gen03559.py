from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy("started_at")

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_semi")
)

deduped = (
    eligible
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

matched_keys = (
    deduped
    .select("customer_id", "device")
    .exceptAll(
        df_sessions
        .filter(~F.col("browser").isin(["Safari", "Firefox", "Chrome"]))
        .select("customer_id", "device")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("device", F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .orderBy(F.col("num_devices").desc_nulls_last())
)
