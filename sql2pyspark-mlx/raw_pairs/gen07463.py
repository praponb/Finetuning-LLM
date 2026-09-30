from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_avg_2").desc())
w5 = Window.partitionBy("order_id").orderBy("cumulative_shipping_fee", "id")
w_part = Window.partitionBy("order_id")

joined = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        F.col("b.order_date"),
        F.col("b.shipping_fee"),
    )
)

joined_order_items = (
    joined
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .filter(F.lower(F.col("b.product_name")).like("%oga%"))
    .select("a.*", F.col("b.product_name"))
)

enriched = (
    joined_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "order_date",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_2"),
        F.sum("shipping_fee").over(w_grp).alias("cumulative_shipping_fee"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "product_id",
        "cumulative_quantity",
        "cumulative_shipping_fee",
        F.percent_rank().over(w4).alias("quantity_avg_2_percent_rank"),
        F.lag("cumulative_quantity", 2).over(w5).alias("prev_cumulative_quantity"),
        F.min("cumulative_quantity").over(w_part).alias("order_id_min_cumulative_quantity"),
    )
)

joined_order_items_2 = (
    metrics
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.cumulative_quantity",
        "a.quantity_avg_2_percent_rank",
        "a.prev_cumulative_quantity",
        "a.order_id_min_cumulative_quantity",
        F.col("p.brand"),
        F.col("p.cost"),
    )
)

result = (
    joined_order_items_2
    .groupBy(
        F.when(F.col("quantity_avg_2_percent_rank") >= 0.68, "HIGH").otherwise("LOW").alias("quantity_avg_2_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.percentile_approx("cumulative_quantity", [0.25, 0.5, 0.75]).alias("cumulative_quantity_quartiles"),
    )
    .orderBy(F.col("quantity_avg_2_percent_rank_tier").desc_nulls_first())
)
