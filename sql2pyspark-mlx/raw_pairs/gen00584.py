from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("session_id").desc(), "id", "key")
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").desc(), "id", "key")
w5 = Window.partitionBy("host").orderBy("row_num", "id", "key")

exploded = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode("context").alias("key", "v"))
)

flagged = (
    exploded
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

latest = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

latest_app_logs = (
    ranked
    .withColumn("row_num_2", F.row_number().over(w5))
    .filter(F.col("row_num_2") == 1)
    .select("id", "host", "key", "rn")
)

daily = (
    latest_app_logs
    .cube("host")
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("host").alias("grouping_level"),
    )
    .filter(F.col("version_rows") < 1000)
)

result = (
    daily
    .select("host", "region_events", "version_rows", "distinct_ids", "grouping_level")
    .orderBy(F.col("grouping_level").desc_nulls_first())
)
