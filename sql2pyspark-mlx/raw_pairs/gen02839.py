from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w_running = Window.partitionBy("brand").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("unit_price_whole", F.floor("unit_price"))
)

enriched = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category") == "Fashion")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.cost"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "quantity",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.last("cost", ignorenulls=True).over(w_running).alias("last_known_cost"),
    )
)

enriched_order_items = (
    with_windows
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.discount"), F.col("o.status"))
)

result = (
    enriched_order_items
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quantity_tier", "status"], ["quantity_tier"], []],
        "quantity_tier",
        "status",
    )
    .agg(
        F.percentile_approx("last_known_cost", [0.25, 0.5, 0.75]).alias("last_known_cost_quartiles"),
        F.max("discount").alias("max_discount"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.grouping("quantity_tier").alias("grouping_level"),
    )
    .orderBy(F.col("quantity_tier").desc_nulls_last())
)
