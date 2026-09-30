from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy(F.col("last3_avg_unit_price").asc_nulls_first(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

joined = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.order_date"), F.col("p.shipping_fee"))
)

enriched = (
    joined
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "order_date",
        F.min("unit_price").over(w).alias("last3_min_unit_price"),
        F.avg("unit_price").over(w).alias("last3_avg_unit_price"),
        F.count("unit_price").over(w).alias("last3_count_unit_price"),
        F.sum("unit_price").over(w).alias("last3_sum_unit_price"),
        F.corr("unit_price", "quantity").over(w2).alias("corr_unit_price_quantity"),
    )
)

base = (
    enriched
    .filter(F.col("last3_sum_unit_price") > 1700)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "order_date",
        "last3_min_unit_price",
        "last3_avg_unit_price",
        "last3_count_unit_price",
        "last3_sum_unit_price",
        "corr_unit_price_quantity",
        F.round("last3_min_unit_price", 1).alias("last3_min_unit_price_rounded"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "order_date",
        "last3_min_unit_price",
        "last3_count_unit_price",
        "last3_sum_unit_price",
        "corr_unit_price_quantity",
        F.col("min_shipment_cost"),
    )
)
