from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w4 = Window.partitionBy("k").orderBy("burst_id", "id", "k")
w5 = Window.orderBy(F.col("burst_id").desc(), "id", "k")
w_hist = Window.partitionBy("k").orderBy("row_seq", "id", "k")

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

gaps = (
    exploded
    .withColumn("is_new_burst", F.when(F.lag("created_at").over(w).isNull() | (F.col("created_at") > F.lag("created_at").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "k",
        "value",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

scored = (
    islands
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("burst_id_bucket", F.ntile(5).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

enriched = (
    scored
    .filter(F.col("row_seq").between(5, 7))
    .withColumn("prev_burst_id", F.lag("burst_id").over(w_hist))
    .withColumn("next_burst_id", F.lead("burst_id").over(w_hist))
    .withColumn("prev_row_seq", F.lag("row_seq").over(w_hist))
    .withColumn("next_row_seq", F.lead("row_seq").over(w_hist))
    .withColumn("row_seq_2", F.row_number().over(w_hist))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("burst_id_bucket") >= 7, "LARGE").otherwise("SMALL").alias("burst_id_bucket_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.percentile_approx("burst_id_bucket", 0.9).alias("burst_id_bucket_p90"),
        F.count(F.when(F.col("k") == "weight_kg", 1)).alias("weight_kg_count"),
        F.percentile_approx("row_seq_2", 0.95).alias("row_seq_2_p95"),
    )
    .filter(F.col("row_seq_2_p95") > 0)
)
