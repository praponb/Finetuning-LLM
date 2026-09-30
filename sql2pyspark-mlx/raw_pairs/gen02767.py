from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("brand", "product_id")
w4 = Window.partitionBy("order_id").orderBy("unit_price_share_2", "id").rowsBetween(-2, -1)
w5 = Window.orderBy("unit_price_share_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("brand_count_unit_price", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        F.col("p.brand"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4).alias("unit_price_share"),
        F.count("unit_price").over(w2).alias("brand_count_unit_price"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share_2"),
    )
)

metrics = (
    enriched_order_items
    .select(
        "*",
        F.avg("unit_price_share_2").over(w4).alias("unit_price_share_2_avg_2_prior"),
        F.sum("unit_price_share_2").over(w5).alias("running_unit_price_share_2"),
        F.lag("unit_price_share", 1).over(w6).alias("prev_unit_price_share"),
    )
)

filtered = (
    metrics
    .filter(F.col("prev_unit_price_share") > 0.68)
    .withColumn("brand_count_unit_price_times_running_unit_price_share_2", (F.col("brand_count_unit_price") * F.col("running_unit_price_share_2")))
)

result = (
    filtered
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "unit_price_share",
        "unit_price_share_2",
        "unit_price_share_2_avg_2_prior",
        "prev_unit_price_share",
        "brand_count_unit_price_times_running_unit_price_share_2",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("unit_price_share").desc(), "unit_price_share_2")
)
