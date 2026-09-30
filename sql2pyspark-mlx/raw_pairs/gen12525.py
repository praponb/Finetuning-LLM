from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "attr_key").orderBy("created_at", "id", "attr_key")
w2 = Window.orderBy(F.col("created_at").desc(), "id", "attr_key")
w3 = Window.orderBy("created_at", "id", "attr_key")
w_grp = Window.partitionBy("attr_key").orderBy(F.col("seq_no_whole").desc_nulls_last())
w5 = Window.partitionBy(F.to_date("created_at"), "attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_running = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.partitionBy("attr_key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

enriched = (
    flattened
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
)

base = (
    enriched
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_value",
        "seq_no_2",
        F.ceil("seq_no").alias("seq_no_whole"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_clean"),
    )
)

kept_keys = (
    base
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("created_at") >= "2024-02-24")
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_carts = (
    reconciled
    .withColumn("seq_no_whole_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("seq_no_whole_max_15m", F.max("seq_no_whole").over(w5))
    .withColumn("seq_no_2_count_1h", F.count("seq_no_2").over(w_running))
    .withColumn("seq_no_whole_avg_30m", F.avg("seq_no_whole").over(w7))
)

cleaned = (
    enriched_carts
    .filter(F.col("attr_key").isNotNull() & (F.col("created_at") > F.lit("2025-03-01 19:00:00").cast("timestamp")))
    .select("*", F.date_trunc("WEEK", "created_at").alias("created_at_period"))
)

enriched_carts_2 = (
    cleaned
    .filter(F.col("attr_key") != "weight_kg")
    .select(
        "id",
        "created_at",
        "attr_key_clean",
        "seq_no_whole_avg_30m",
        "created_at_period",
        F.round((F.col("seq_no_2_count_1h") / F.sum("seq_no_2_count_1h").over(w8)), 4).alias("seq_no_2_count_1h_share"),
    )
)

result = (
    enriched_carts_2
    .groupBy(
        F.when(F.col("seq_no_whole_avg_30m") >= 73, "HIGH").otherwise("LOW").alias("seq_no_whole_avg_30m_tier"),
    )
    .agg(
        F.round(F.avg("seq_no_2_count_1h_share"), 2).alias("avg_seq_no_2_count_1h_share"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("avg_seq_no_2_count_1h_share").desc())
)
