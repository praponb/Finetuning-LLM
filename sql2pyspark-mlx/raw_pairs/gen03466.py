from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc())
w_running = Window.partitionBy("carrier").orderBy("row_num", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("shipping_fee", "id")

ranked = (
    df_shipments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.shipped_date",
        "s.status",
        "s.row_num",
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
)

metrics = (
    enriched
    .filter(F.col("shipped_date") <= "2024-10-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "order_date",
        F.avg("shipping_fee").over(w_running).alias("shipping_fee_avg_5_prior"),
        F.lead("shipping_fee", 2, 0).over(w3).alias("next_shipping_fee"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        F.col("b.discount"),
        F.col("b.amount"),
    )
)

remaining_keys = (
    joined
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2024-12-13", "2025-05-01"))
        .select("order_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

grouped = (
    verified
    .rollup("order_id")
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.grouping("order_id").alias("grouping_id"),
    )
)

result = grouped.select("order_id", "discount_quartiles", "avg_amount", "grouping_id")
