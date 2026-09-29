from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("cost", "id")
w3 = Window.partitionBy("status").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("cost_bucket", "id")
w5 = Window.partitionBy("status")

valid = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-06-01")
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-06-27"))) & ~F.col("status").isin(["DELIVERED", "RETURNED"]))
    .withColumn("cost_minus_rank_in_group", (F.col("cost") - F.col("rank_in_group")))
)

metrics = (
    base
    .select(
        "*",
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.sum("cost_minus_rank_in_group").over(w3).alias("cumulative_cost_minus_rank_in_group"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lead("cost").over(w_recent).alias("next_cost"),
        F.lead("cost_minus_rank_in_group").over(w_recent).alias("next_cost_minus_rank_in_group"),
        F.lag("cost_minus_rank_in_group").over(w_recent).alias("prev_cost_minus_rank_in_group"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.count("cost_bucket").over(w5).alias("status_count_cost_bucket"),
    )
)

summary = (
    scored
    .groupBy("status")
    .agg(
        F.max(F.when(F.col("status") == "RETURNED", F.col("next_cost_minus_rank_in_group"))).alias("returned_max_next_cost_minus_rank_in_group"),
        F.percentile_approx("next_cost", 0.95).alias("next_cost_p95"),
        F.percentile_approx("prev_cost_minus_rank_in_group", 0.5).alias("prev_cost_minus_rank_in_group_p50"),
    )
)

result = (
    summary
    .select("status", "next_cost_p95")
    .orderBy(F.col("next_cost_p95").desc_nulls_first())
)
