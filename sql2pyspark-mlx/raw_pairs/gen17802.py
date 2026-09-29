from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service").orderBy("status_code", "id", "k")
w2 = Window.partitionBy("host", "service").orderBy("log_ts", "id", "k")
w_hist = Window.partitionBy("service").orderBy("status_code", "id", "k")
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("host", "service").orderBy(F.col("prev_status_code").asc_nulls_first(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

deduped = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .filter(F.hour("log_ts").between(9, 13))
    .withColumn("prev_status_code", F.lag("status_code").over(w_hist))
    .withColumn("next_status_code", F.lead("status_code").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("status_code_count_2h", F.count("status_code").over(w4))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

stats = (
    latest
    .groupBy("host")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
)

result = (
    stats
    .select("host", "started_at", "host_list")
    .orderBy(F.col("started_at").desc())
)
