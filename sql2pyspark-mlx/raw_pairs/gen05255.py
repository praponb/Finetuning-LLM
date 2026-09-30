from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("order_date"), "channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

enriched = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "discount",
        F.sum("discount").over(w).alias("running_discount"),
        F.sum("discount").over(w2).alias("cumulative_discount"),
        F.avg("discount").over(w3).alias("discount_avg_6_prior"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "cumulative_discount",
        "discount_avg_6_prior",
    )
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isNull()), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.cost"),
    )
)

result = (
    with_last
    .select(
        "id",
        "order_date",
        "cumulative_discount",
        "discount_avg_6_prior",
        "shipment_shipped_date",
        "cost",
    )
    .orderBy(F.col("cost").asc_nulls_first())
)
