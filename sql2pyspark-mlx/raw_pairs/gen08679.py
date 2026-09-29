from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc_nulls_first(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

ranked = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_dims = (
    ranked
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.store_id") == 7)
    .select(
        "e.id",
        "e.order_id",
        "e.delivered_date",
        "e.cost",
        "e.status",
        F.col("p.amount"),
        F.col("p.shipping_fee"),
        F.col("p.status").alias("order_status"),
    )
)

with_dims_shipments = (
    with_dims
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.delivered_date",
        "e.cost",
        "e.amount",
        "e.shipping_fee",
        "e.order_status",
        F.col("b.order_date"),
        F.col("b.discount"),
        F.col("b.shipping_fee").alias("order_shipping_fee"),
    )
)

enriched = (
    with_dims_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost",
        "src.shipping_fee",
        "src.order_status",
        "src.discount",
        F.col("o.discount").alias("order_discount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.order_date").alias("order_order_date"),
    )
)

grouped = (
    enriched
    .groupBy("order_id", F.date_trunc("year", "order_order_date").alias("year_start"))
    .agg(
        F.max("order_discount").alias("max_order_discount"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_count"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.avg(F.when(F.col("order_status") == "CANCELLED", F.col("cost"))).alias("cancelled_avg_cost"),
    )
    .filter(F.col("num_order_status") >= 1)
)

result = (
    grouped
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "year_start",
        "max_order_discount",
        "cost_quartiles",
        "num_order_status",
        "cancelled_avg_cost",
        F.col("max_payment_amount"),
    )
)
