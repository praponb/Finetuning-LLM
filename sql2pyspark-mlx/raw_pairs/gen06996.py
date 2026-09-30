from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "key")
w_part = Window.partitionBy("level").orderBy("dedup_rank", "id", "key").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("level", "key").orderBy(F.col("last7_max_status_code").desc(), "id", "key")

lines = (
    df_app_logs
    .select("id", "level", "status_code", F.explode("context").alias("key", "value"))
)

ranked = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("last7_min_status_code", F.min("status_code").over(w_part))
    .withColumn("last7_max_status_code", F.max("status_code").over(w_part))
    .withColumn("last7_count_status_code", F.count("status_code").over(w_part))
)

ranked_app_logs = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_app_logs
    .select(
        "id",
        "key",
        "value",
        "dedup_rank",
        "last7_count_status_code",
        F.when(F.col("key").rlike("^(version|trace)$"), "EXTENDED").when(F.col("key").rlike("^(region)$"), "PRIORITY").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("key").desc_nulls_first())
)
