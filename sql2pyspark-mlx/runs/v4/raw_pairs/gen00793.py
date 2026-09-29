from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")
w_running = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")
w3 = Window.orderBy("created_at", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(-3, -1)
w5 = Window.partitionBy("attr_key").orderBy("event_seq_2", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy(F.col("event_seq").desc(), "id", "attr_key")
w7 = Window.orderBy("row_seq", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("attr_key")
w9 = Window.partitionBy("attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w10 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

enriched = (
    flattened
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("event_seq_2", F.row_number().over(w_running))
    .withColumn("row_seq", F.row_number().over(w3))
)

enriched_carts = (
    enriched
    .select(
        "*",
        F.avg("row_seq").over(w4).alias("row_seq_avg_3_prior"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.row_number().over(w6).alias("seq_no_3"),
    )
)

scored = (
    enriched_carts
    .select(
        "*",
        F.sum("row_seq_avg_3_prior").over(w7).alias("cumulative_row_seq_avg_3_prior"),
        F.avg("event_seq_2").over(w8).alias("attr_key_avg_event_seq_2"),
        F.avg("seq_no").over(w9).alias("seq_no_avg_2h"),
    )
)

with_prev = scored.withColumn("prev_created_at", F.lag("created_at").over(w_running))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_created_at").isNull() | (F.col("created_at") > F.col("prev_created_at") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w10))

stats = (
    islands
    .groupBy(F.date_trunc("YEAR", "prev_created_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_count"),
        F.count_if(F.col("attr_key") == "gift").alias("gift_events"),
        F.count_if(F.col("attr_key") == "utm_source").alias("utm_source_events"),
        F.max(F.when(F.col("attr_key") == "gift", F.col("row_seq_avg_3_prior"))).alias("gift_max_row_seq_avg_3_prior"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "device_rows",
        "row_count",
        "weight_kg_count",
        "gift_events",
        "gift_max_row_seq_avg_3_prior",
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
