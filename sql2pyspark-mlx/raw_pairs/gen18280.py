from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.month("launch_date"), "supplier_id").orderBy("launch_date", "id")
w_recent = Window.partitionBy("category", "supplier_id").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("brand").orderBy(F.col("order_item_unit_price").asc_nulls_first())
w7 = Window.orderBy(F.col("cost_cume_dist").desc(), "id")

valid = (
    df_products
    .filter((F.col("brand").eqNullSafe(F.lit("Initech"))) & (F.col("launch_date") >= "2024-01-01"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.sort_array("tags").alias("sorted_tags"),
        F.trim("brand").alias("clean_brand"),
    )
)

enriched = (
    valid
    .filter(F.col("brand") == "Initech")
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "tags",
        "sorted_tags",
        F.lag("price", 2, 0).over(w_part).alias("prev_price"),
        F.avg("price").over(w_recent).alias("price_avg_5"),
        F.cume_dist().over(w3).alias("cost_cume_dist"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.tags",
        "s.sorted_tags",
        "s.price_avg_5",
        "s.cost_cume_dist",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([5, 6, 7, 9])), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") >= 7), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

enriched_products = (
    with_last
    .select(
        "id",
        "sorted_tags",
        "price_avg_5",
        F.cume_dist().over(w6).alias("order_item_unit_price_cume_dist"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    enriched_products
    .select("id", "sorted_tags", "order_item_unit_price_cume_dist", "seq_no")
    .orderBy(F.col("seq_no").desc())
)
