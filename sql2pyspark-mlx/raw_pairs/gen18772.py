from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service").orderBy(F.col("log_ts").desc(), "id", "k")
w2 = Window.partitionBy("level", "k").orderBy("log_ts", "id", "k")
w3 = Window.partitionBy("k").orderBy(F.col("status_code_num").desc(), "id", "k")
w4 = Window.partitionBy("level", "k").orderBy("seq_no_2", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

ranked = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "log_ts", "level", "status_code", "k", "v")
)

filtered = (
    ranked
    .filter(F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp"))
    .select(
        "*",
        F.hour("log_ts").alias("log_ts_hour"),
        F.col("status_code").cast("double").alias("status_code_num"),
    )
)

enriched = (
    filtered
    .filter(F.col("status_code") >= 372)
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "status_code",
        "k",
        "v",
        "status_code_num",
        "seq_no",
        "seq_no_2",
        F.lead("seq_no_2").over(w4).alias("next_seq_no_2"),
        F.lag("seq_no_2").over(w4).alias("prev_seq_no_2"),
        F.lead("seq_no").over(w4).alias("next_seq_no"),
        F.row_number().over(w4).alias("seq_no_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "status_code",
        "k",
        "v",
        "status_code_num",
        "seq_no",
        "next_seq_no_2",
        "prev_seq_no_2",
        "next_seq_no",
        F.when(F.col("k").rlike("^(version|trace)$"), "EXTENDED").when(F.col("k").rlike("^(region)$"), "OTHER_ZONE").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("seq_no").desc_nulls_first(), "next_seq_no")
)
