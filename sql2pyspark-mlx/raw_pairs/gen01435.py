from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("session_id_scaled", "id", "k")
w_hist = Window.partitionBy(F.date_trunc("YEAR", "created_at"), "k").orderBy("created_at", "id", "k")
w_hist_2 = Window.orderBy(F.col("created_at").desc(), "id", "k")

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("k", "value"))

flagged = (
    lines
    .withColumn("session_start_flag", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_carts = (
    flagged
    .select(
        "id",
        "created_at",
        "k",
        "value",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

valid = (
    sessions_carts
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select(
        "*",
        F.substring("k", 1, 2).alias("k_prefix"),
        F.round((F.col("session_id") * 1.1), 2).alias("session_id_scaled"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.ntile(3).over(w3).alias("session_id_scaled_bucket"),
        F.lag("session_id_scaled", 1).over(w_hist).alias("prev_session_id_scaled"),
        F.row_number().over(w_hist_2).alias("row_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
        "session_id",
    )
    .agg(
        F.round(F.sum("prev_session_id_scaled") / F.nullif(F.count(F.when(F.col("k") == "utm_source", 1)), F.lit(0)), 2).alias("prev_session_id_scaled_per_utm_source"),
        F.max("created_at").alias("last_seen"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("k") == "device", 1)), F.lit(0)), 2).alias("row_seq_per_device"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.sum(F.when(F.col("k") == "device", F.col("prev_session_id_scaled")).otherwise(0)).alias("device_prev_session_id_scaled"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("k") == "weight_kg", 1)), F.lit(0)), 2).alias("row_seq_per_weight_kg"),
    )
    .filter(F.col("device_prev_session_id_scaled") > 0)
    .orderBy("device_prev_session_id_scaled")
)
