from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_discount").desc(), "id")
w_part = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy(F.month("shipped_date"), "order_id").orderBy("cost", "id")
w_running = Window.partitionBy("status", "order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("first_known_order_discount").desc_nulls_last())

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.discount") >= 0.3)
    .select("e.*", F.col("b.discount"), F.col("b.shipping_fee"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.shipped_date",
        "src.delivered_date",
        "src.cost",
        "src.status",
        "src.discount",
        F.col("p.discount").alias("order_discount"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched_shipments = (
    ranked
    .select(
        "*",
        F.row_number().over(w_part).alias("row_seq"),
        F.lead("order_discount", 1, 0).over(w3).alias("next_order_discount"),
        F.first("order_discount", ignorenulls=True).over(w_running).alias("first_known_order_discount"),
    )
)

kept_keys = (
    enriched_shipments
    .select("order_id", "status")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isin([5, 6, 12, 24]))
        .select("order_id", "status")
    )
)

verified = (
    enriched_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("delivered_date") >= F.lit("2023-03-31").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "discount",
        "order_discount",
        "next_order_discount",
        "first_known_order_discount",
        F.when(F.col("status").isin(["IN_TRANSIT", "RETURNED"]), "Core").otherwise("Other").alias("status_group"),
    )
)

with_windows = (
    base
    .select(
        "discount",
        "next_order_discount",
        "first_known_order_discount",
        F.rank().over(w5).alias("first_known_order_discount_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("discount") >= 0.33, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        F.when(F.col("first_known_order_discount_rank") >= 7, "HIGH").otherwise("LOW").alias("first_known_order_discount_rank_tier"),
    )
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.max("first_known_order_discount_rank").alias("max_first_known_order_discount_rank"),
        F.count("*").alias("cnt"),
        F.round(F.avg("discount"), 2).alias("avg_discount_2"),
        F.percentile_approx("first_known_order_discount", [0.25, 0.5, 0.75]).alias("first_known_order_discount_quartiles"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("avg_discount_2") >= 0)
    .orderBy("avg_discount")
)
