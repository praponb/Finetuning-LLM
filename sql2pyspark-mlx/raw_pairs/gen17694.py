from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("launch_date", "id")
w3 = Window.partitionBy("category").orderBy(F.col("order_item_quantity").desc_nulls_last())
w4 = Window.partitionBy(F.year("launch_date"), "category").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") < "2024-04-01"))
    .withColumn("tags_count", F.size("tags"))
)

kept = (
    filtered
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([3, 8, 10])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.tags_count",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

cleaned = (
    latest_match
    .filter(~F.col("product_name").like("%Coff%"))
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "launch_date",
        "unit_price",
        "order_item_quantity",
        F.least("unit_price", "order_item_quantity").alias("least_unit_price_order_item_quantity"),
        F.regexp_replace("product_name", " ", "_").alias("product_name_slug"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 4])), "left_semi")
)

scored = (
    eligible
    .select(
        "id",
        "product_name",
        "cost",
        "unit_price",
        "least_unit_price_order_item_quantity",
        "product_name_slug",
        F.lag("order_item_quantity", 1).over(w2).alias("prev_order_item_quantity"),
        F.cume_dist().over(w3).alias("order_item_quantity_cume_dist"),
        F.sum("unit_price").over(w4).alias("running_unit_price"),
    )
)

result = (
    scored
    .withColumn("prev_order_item_quantity_tier", F.when(F.col("prev_order_item_quantity") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 540, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_order_item_quantity_tier", "cost_tier"], ["prev_order_item_quantity_tier"]],
        "prev_order_item_quantity_tier",
        "cost_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("order_item_quantity_cume_dist").alias("avg_order_item_quantity_cume_dist"),
        F.percentile_approx("running_unit_price", 0.95).alias("running_unit_price_p95"),
        F.percentile_approx("running_unit_price", 0.95).alias("running_unit_price_p95_2"),
    )
)
