from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_listener_id_device_by_ms_pla_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_listener_id_device_by_ms_pla_3 = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc())

prepared = (
    df_plays
    .filter(F.col("source").isin("album", "radio", "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

ranked = (
    prepared
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        F.row_number().over(w_listener_id_device_by_ms_pla).alias("seq_no"),
        F.lead("played_at", 3).over(w_all_by_ms_played).alias("next3_played_at"),
        F.sum("ms_played").over(w_listener_id_device_by_ms_pla_2).alias("ms_played_sum_7"),
        F.dense_rank().over(w_listener_id_device_by_ms_pla_3).alias("ms_played_dense_rank"),
        F.ntile(10).over(w_all_by_ms_played).alias("ms_played_decile"),
    )
)

annotated = (
    ranked
    .filter(F.col("ms_played_dense_rank") > 2)
    .select(
        "*",
        F.month(F.col("next3_played_at")).alias("next3_played_at_month"),
        F.when(F.col("seq_no") >= 8, "large").when(F.col("seq_no") >= 5, "medium").otherwise("small").alias("seq_no_tier"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

result = (
    annotated
    .groupBy("next3_played_at")
    .pivot("seq_no_tier", ["medium", "small"])
    .agg(F.sum("seq_no"))
)
