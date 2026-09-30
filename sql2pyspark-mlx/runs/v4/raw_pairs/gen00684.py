from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "level").orderBy(F.col("status_code").desc(), "id", "key")
w2 = Window.partitionBy("level").orderBy(F.col("latest_rank").desc())
w3 = Window.partitionBy("level", "host").orderBy(F.col("rank_in_group").desc())
w4 = Window.partitionBy("host", "level").orderBy("rank_in_group", "id", "key").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("key").orderBy("status_code", "id", "key")
w6 = Window.partitionBy("level", "key")
w7 = Window.orderBy(F.col("status_code_bucket").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

ranked = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest
    .withColumn("status_code_avg_2", F.avg("status_code").over(w4))
    .withColumn("status_code_bucket", F.ntile(5).over(w5))
)

kept_keys = (
    scored
    .select("id", "host")
    .intersect(
        df_app_logs
        .filter(F.to_date("log_ts") < "2023-07-01")
        .select("id", "host")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

scored_app_logs = (
    verified
    .select(
        "*",
        F.avg("status_code_bucket").over(w6).alias("level_avg_status_code_bucket"),
        F.dense_rank().over(w7).alias("status_code_bucket_dense_rank"),
    )
)

result = (
    scored_app_logs
    .select("id", "host", "level", "key", "status_code_avg_2")
    .orderBy(F.col("status_code_avg_2").desc_nulls_last(), F.col("host").desc())
)
