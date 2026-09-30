from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date_plus_months", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-04-19"))) & F.col("order_id").isNotNull() & (F.col("order_id") == 8))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.add_months("shipped_date", 6).alias("shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

kept_keys = (
    with_windows
    .select("carrier")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isin([1, 14]))
        .select("carrier")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.order_date"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.order_date").alias("order_order_date"), F.col("p.discount"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("row_seq") >= 8, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
        F.max(F.when(F.col("status") == "IN_TRANSIT", F.col("event_seq"))).alias("in_transit_max_event_seq"),
    )
    .filter(F.col("avg_row_seq") >= 0)
)
