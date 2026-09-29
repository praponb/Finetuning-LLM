from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").asc_nulls_last(), "id")
w_running = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("stock_qty", "id")
w_hist = Window.partitionBy("brand").orderBy("cost", "id")
w5 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("last_known_cost").desc_nulls_last(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

deduped = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "brand", "cost", "stock_qty", "tags", "supplier_id")
)

with_windows = (
    deduped
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w_running))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("prev_cost", F.lag("cost", 2).over(w_hist))
)

deduped_products = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    deduped_products
    .filter(F.col("brand") == "Stark")
    .select(
        "*",
        F.round((F.col("stock_qty") * 1.1), 2).alias("stock_qty_scaled"),
        F.when(F.col("cost") >= 720, "High").when(F.col("cost") >= 270, "Medium").otherwise("Low").alias("cost_level"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([4, 10, 26, 35])), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 10])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "brand",
        "cost",
        "tags",
        "supplier_id",
        "row_seq",
        "prev_cost",
        "stock_qty_scaled",
        "brand_filled",
    )
    .orderBy(F.col("row_seq").desc_nulls_last(), "stock_qty_scaled")
)
