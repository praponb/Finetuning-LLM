from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy("first_known_unit_price", "id")
w6 = Window.partitionBy("product_id").orderBy("unit_price_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("category", "product_id")
w8 = Window.partitionBy("product_id").orderBy(F.col("stock_qty").desc())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

scored = (
    df_order_items
    .select(
        "*",
        F.avg("unit_price").over(w).alias("unit_price_avg_2_prior"),
        F.first("unit_price", ignorenulls=True).over(w_hist).alias("first_known_unit_price"),
        F.avg("quantity").over(w3).alias("quantity_avg_3_prior"),
        F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4).alias("quantity_share"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.category"), F.col("p.stock_qty"), F.col("p.cost"))
)

scored_order_items = (
    enriched
    .select(
        "*",
        F.lag("unit_price", 1).over(w5).alias("prev_unit_price"),
        F.last("category", ignorenulls=True).over(w6).alias("last_known_category"),
        F.round((F.col("quantity") / F.sum("quantity").over(w7)), 4).alias("quantity_share_2"),
    )
)

ranked = (
    scored_order_items
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_avg_2_prior",
        "first_known_unit_price",
        "quantity_avg_3_prior",
        "stock_qty",
        "prev_unit_price",
        F.col("min_inventory_quantity"),
    )
)
