from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("created_at"), "k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id", "k")
w3 = Window.orderBy(F.col("created_at").desc(), "id", "k")
w4 = Window.orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("event_seq_2", "id", "k")
w6 = Window.partitionBy("k").orderBy(F.col("next_event_seq").desc_nulls_first(), "id", "k")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("k", "value"),
    )
)

with_windows = (
    exploded
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w4))
    .withColumn("event_seq_2", F.row_number().over(w3))
)

enriched = (
    with_windows
    .withColumn("prev_event_seq_2", F.lag("event_seq_2").over(w5))
    .withColumn("next_event_seq_2", F.lead("event_seq_2").over(w5))
    .withColumn("next_event_seq", F.lead("event_seq").over(w5))
    .withColumn("prev_event_seq", F.lag("event_seq").over(w5))
    .withColumn("row_seq_2", F.row_number().over(w5))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .groupBy(
        F.date_trunc("day", "created_at").alias("activity_day"),
        F.when(F.col("prev_event_seq") >= 7, "HIGH").otherwise("LOW").alias("prev_event_seq_tier"),
        "customer_id",
    )
    .agg(
        F.min("event_seq_2").alias("min_event_seq_2"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.avg(F.when(F.col("k") == "weight_kg", F.col("prev_event_seq"))).alias("weight_kg_avg_prev_event_seq"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_count"),
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events"),
    )
    .orderBy(F.col("num_ks").desc())
)
