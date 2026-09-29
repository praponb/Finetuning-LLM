from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("delivered_date"), "order_id")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy("status", "carrier")
w_recent = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("status").orderBy(F.col("status_sum_cost").desc())
w6 = Window.partitionBy("carrier")
w7 = Window.orderBy(F.col("carrier_avg_cost_per_cost_share").desc_nulls_last())
w8 = Window.orderBy(F.col("status_sum_cost_share").desc())
w9 = Window.orderBy(F.col("order_id_max_cost_per_cost_share").asc_nulls_last())

matched_keys = (
    df_shipments
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2023-03-03", "2025-05-23"))
        .select("id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share_2"),
        F.sum("cost").over(w3).alias("status_sum_cost"),
    )
)

valid = (
    scored
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-04-16")))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "cost_share_2",
        "status_sum_cost",
        (F.col("cost") / F.nullif("cost_share", F.lit(0))).alias("cost_per_cost_share"),
        F.trim("status").alias("clean_status"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "cost",
        "cost_share_2",
        "clean_status",
        "resolved_carrier",
        F.max("cost_per_cost_share").over(w_recent).alias("order_id_max_cost_per_cost_share"),
        F.rank().over(w_ordered).alias("status_sum_cost_rank"),
        F.avg("cost_per_cost_share").over(w6).alias("carrier_avg_cost_per_cost_share"),
        F.count("cost_per_cost_share").over(w3).alias("status_count_cost_per_cost_share"),
        F.round((F.col("status_sum_cost") / F.sum("status_sum_cost").over(w6)), 4).alias("status_sum_cost_share"),
    )
)

scored_shipments = (
    with_windows
    .select(
        "id",
        "clean_status",
        "resolved_carrier",
        "order_id_max_cost_per_cost_share",
        "carrier_avg_cost_per_cost_share",
        F.dense_rank().over(w7).alias("carrier_avg_cost_per_cost_share_dense_rank"),
        F.dense_rank().over(w8).alias("status_sum_cost_share_dense_rank"),
        F.dense_rank().over(w9).alias("order_id_max_cost_per_cost_share_dense_rank"),
    )
)

result = (
    scored_shipments
    .withColumn("carrier_avg_cost_per_cost_share_dense_rank_tier", F.when(F.col("carrier_avg_cost_per_cost_share_dense_rank") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("order_id_max_cost_per_cost_share_dense_rank_tier", F.when(F.col("order_id_max_cost_per_cost_share_dense_rank") >= 9, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["carrier_avg_cost_per_cost_share_dense_rank_tier", "order_id_max_cost_per_cost_share_dense_rank_tier"], ["carrier_avg_cost_per_cost_share_dense_rank_tier"]],
        "carrier_avg_cost_per_cost_share_dense_rank_tier",
        "order_id_max_cost_per_cost_share_dense_rank_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("status_sum_cost_share_dense_rank"), 2).alias("avg_status_sum_cost_share_dense_rank"),
    )
)
