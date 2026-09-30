from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("cost").desc())
w4 = Window.partitionBy("carrier").orderBy("cost_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier", "order_id")
w6 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("cost_dense_rank", "id")
w8 = Window.partitionBy("status").orderBy("first_known_cost_dense_rank", "id")
w_part = Window.partitionBy("order_id").orderBy("seq_no_2", "id")
w10 = Window.partitionBy("order_id", "status").orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("order_id").orderBy("seq_no_2", "id").rowsBetween(-2, Window.currentRow)
w12 = Window.partitionBy("order_id").orderBy(F.col("prev_carrier_max_cost_dense_rank").desc_nulls_last(), "id")
w13 = Window.orderBy(F.col("prev_carrier_max_cost_dense_rank").desc(), "id")
w14 = Window.partitionBy("order_id", "carrier").orderBy("prev_carrier_max_cost_dense_rank", "id")

filtered = (
    df_shipments
    .filter(F.col("cost") <= 84)
    .select(
        "*",
        F.when(F.col("status").isin(["RETURNED", "DELIVERED"]), "Group A").otherwise("Other").alias("status_group"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

scored = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("cost_dense_rank", F.dense_rank().over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.first("cost_dense_rank", ignorenulls=True).over(w4).alias("first_known_cost_dense_rank"),
        F.max("cost_dense_rank").over(w5).alias("carrier_max_cost_dense_rank"),
    )
)

metrics_shipments = (
    metrics
    .withColumn("cumulative_cost_cume_dist", F.sum("cost_cume_dist").over(w6))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("prev_carrier_max_cost_dense_rank", F.lag("carrier_max_cost_dense_rank", 1).over(w8))
)

scored_shipments = (
    metrics_shipments
    .withColumn("next_prev_carrier_max_cost_dense_rank", F.lead("prev_carrier_max_cost_dense_rank", 2).over(w_part))
    .withColumn("last_known_first_known_cost_dense_rank", F.last("first_known_cost_dense_rank", ignorenulls=True).over(w10))
    .withColumn("seq_no_avg_2", F.avg("seq_no").over(w11))
    .withColumn("carrier_min_cost_dense_rank", F.min("cost_dense_rank").over(w5))
    .withColumn("first_known_cost_dense_rank_share", F.round((F.col("first_known_cost_dense_rank") / F.sum("first_known_cost_dense_rank").over(w5)), 4))
)

deduped = (
    scored_shipments
    .withColumn("dedup_rank", F.row_number().over(w12))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .withColumn("prev_carrier_max_cost_dense_rank_bucket", F.ntile(5).over(w13))
    .withColumn("next_carrier_max_cost_dense_rank", F.lead("carrier_max_cost_dense_rank", 2).over(w14))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "status_group",
        "carrier_clean",
        "seq_no",
    )
    .orderBy(F.col("order_id").desc())
)
