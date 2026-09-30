from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy(F.col("cost_percent_rank").desc_nulls_last())
w4 = Window.partitionBy("order_id").orderBy(F.col("cost_percent_rank").desc())
w5 = Window.partitionBy("order_status").orderBy(F.col("cost_share").desc_nulls_first())

with_dims = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.status").alias("order_status"), F.col("b.channel"))
)

remaining_keys = (
    with_dims
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("carrier").isin(["FedEx", "UPS"]))
        .select("id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.percent_rank().over(w).alias("cost_percent_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

cleaned = (
    enriched
    .filter(F.col("delivered_date") >= F.lit("2024-03-29").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "order_status",
        "channel",
        "cost_percent_rank",
        "cost_share",
        F.round((F.col("cost_percent_rank") * 1.07), 2).alias("cost_percent_rank_adjusted"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.rank().over(w3).alias("cost_percent_rank_rank"),
        F.cume_dist().over(w4).alias("cost_percent_rank_cume_dist"),
        F.percent_rank().over(w5).alias("cost_share_percent_rank"),
    )
)

kept_keys = (
    scored
    .select("order_id", "id")
    .exceptAll(
        df_shipments
        .filter(F.col("shipped_date") < "2024-06-01")
        .select("order_id", "id")
    )
)

kept_rows_shipments = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows_shipments
    .select("status", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isin([18, 23, 38]))
        .select("status", "carrier")
    )
)

verified = (
    kept_rows_shipments
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "order_status",
        F.col("cost_percent_rank_adjusted").alias("COST_PERCENT_RANK_ADJUSTED"),
        "cost_share",
        F.col("cost_percent_rank_cume_dist").alias("COST_PERCENT_RANK_CUME_DIST_VAL"),
    )
    .unpivot(
        ["order_status"],
        ["COST_PERCENT_RANK_ADJUSTED", "cost_share", "COST_PERCENT_RANK_CUME_DIST_VAL"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
