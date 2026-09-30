from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id", "device").orderBy(F.col("is_skipped_flag").desc(), F.col("id").desc())

filtered_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

labeled = (
    filtered_plays
    .select(
        "*",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.when(F.col("source").isin("radio", "playlist"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

latest = (
    labeled
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source_device_label",
        "source_group",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
