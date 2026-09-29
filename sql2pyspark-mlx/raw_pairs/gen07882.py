from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("order_item_quantity").asc_nulls_first())
w_recent = Window.orderBy("launch_date", "id")
w_grp = Window.partitionBy(F.month("launch_date")).orderBy("order_item_quantity", "id")
w_ordered = Window.orderBy("order_item_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("inventory_quantity", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.orderBy(F.col("order_item_quantity").desc(), "id")
w10 = Window.partitionBy(F.year("launch_date")).orderBy("first_known_cost", "id")
w11 = Window.orderBy("order_item_quantity_cume_dist", "id")
w12 = Window.partitionBy(F.year("launch_date")).orderBy(F.col("launch_date").desc(), "id")

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.concat("brand", F.lit(" - "), "product_name").alias("brand_product_name"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

with_last_products = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.brand_product_name",
        "s.inventory_quantity",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

scored = (
    with_last_products
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w3))
    .withColumn("order_item_quantity_cume_dist", F.cume_dist().over(w4))
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("cumulative_inventory_quantity", F.sum("inventory_quantity").over(w_ordered))
)

result = (
    scored
    .select(
        "id",
        "launch_date",
        "tags",
        "brand_product_name",
        "inventory_quantity",
        "order_item_quantity",
        "first_known_cost",
        "order_item_quantity_cume_dist",
        "row_seq",
        "cumulative_inventory_quantity",
        F.avg("next_cost").over(w8).alias("next_cost_avg_2"),
        F.ntile(5).over(w9).alias("order_item_quantity_bucket"),
        F.lag("first_known_cost", 1).over(w10).alias("prev_first_known_cost"),
        F.row_number().over(w11).alias("row_seq_2"),
        F.row_number().over(w12).alias("row_seq_3"),
    )
)
