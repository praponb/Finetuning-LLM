from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy(F.month("launch_date"), "supplier_id")
w3 = Window.partitionBy(F.month("launch_date")).orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("price").between(370, 530))
    .select(
        "*",
        F.lower("category").alias("category_lower"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock").between("2024-08-20", "2024-12-16")), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "brand",
        "price",
        "cost",
        F.min("price").over(w).alias("brand_min_price"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.last("price", ignorenulls=True).over(w3).alias("last_known_price"),
        F.sum("stock_qty").over(w4).alias("cumulative_stock_qty"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

filtered_products = (
    with_latest
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Stark"))))
    .select(
        "*",
        F.greatest("quantity", "cost").alias("greatest_quantity_cost"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

summary = (
    filtered_products
    .withColumn("greatest_quantity_cost_tier", F.when(F.col("greatest_quantity_cost") >= 72, "HIGH").otherwise("LOW"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 7, "HIGH").otherwise("LOW"))
    .cube("greatest_quantity_cost_tier", "quantity_tier")
    .agg(
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.grouping("greatest_quantity_cost_tier").alias("gid"),
    )
    .filter(F.col("avg_unit_price") > 0)
)

result = (
    summary
    .select(
        "greatest_quantity_cost_tier",
        "quantity_tier",
        "cost_p95",
        "avg_unit_price",
        "gid",
    )
    .orderBy(F.col("gid").desc())
)
