from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("carrier", "status").orderBy("cost", "id")
w4 = Window.orderBy(F.col("cost_filled").desc(), "id")
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_filled").desc(), "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w7 = Window.partitionBy("carrier").orderBy("cost_filled_bucket", "id")
w8 = Window.partitionBy("carrier").orderBy(F.col("next_cost_filled").desc())

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("carrier_prefix", F.substring("carrier", 1, 4))
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "carrier",
        "cost",
        "carrier_prefix",
        F.lead("cost_filled", 1).over(w3).alias("next_cost_filled"),
        F.ntile(3).over(w4).alias("cost_filled_bucket"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("carrier").isNotNull())
    .withColumn("row_seq_rounded", F.round("row_seq", 1))
)

metrics_shipments = (
    filtered
    .filter(F.col("carrier") == "Kerry")
    .select(
        "id",
        "carrier",
        "cost",
        "carrier_prefix",
        "next_cost_filled",
        "cost_filled_bucket",
        "row_seq_rounded",
        F.cume_dist().over(w6).alias("cost_cume_dist"),
        F.lead("cost", 1, 0).over(w7).alias("next_cost"),
        F.rank().over(w8).alias("next_cost_filled_rank"),
    )
)

cleaned = (
    metrics_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .withColumn("next_cost_adjusted", F.round((F.col("next_cost") * 1.1), 2))
    .withColumn("next_cost_filled_2", F.coalesce("next_cost", F.lit(0)))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("NONE")))
)

stats = (
    cleaned
    .groupBy(
        F.when(F.col("next_cost_filled") >= 90, "PREMIUM").otherwise("STANDARD").alias("next_cost_filled_tier"),
    )
    .agg(
        F.avg("next_cost_filled_rank").alias("avg_next_cost_filled_rank"),
        F.round(F.avg("next_cost_filled_rank"), 2).alias("avg_next_cost_filled_rank_2"),
        F.round(F.avg("next_cost_filled"), 2).alias("avg_next_cost_filled"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
    )
)

result = (
    stats
    .select(
        "next_cost_filled_tier",
        "avg_next_cost_filled_rank",
        "avg_next_cost_filled",
        "cost_p90",
    )
    .orderBy(F.col("cost_p90").asc_nulls_first())
)
