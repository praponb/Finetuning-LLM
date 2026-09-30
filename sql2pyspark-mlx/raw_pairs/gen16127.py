from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.month("launch_date"), "category")
w3 = Window.partitionBy("category", "brand").orderBy("quantity", "id")
w4 = Window.partitionBy(F.year("last_restock"), "brand").orderBy("price", "id")
w5 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w_running = Window.partitionBy("brand").orderBy(F.col("quantity").desc())
w7 = Window.partitionBy("category").orderBy(F.col("inventory_quantity").desc(), "id")

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

metrics = (
    with_last
    .filter(F.year("launch_date") >= 2025)
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 2).over(w4))
    .withColumn("stock_qty_bucket", F.ntile(10).over(w5))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w_running))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.last_restock")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
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
        F.when(F.col("category").rlike("^(Grocery|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Home)$"), "EXTENDED").otherwise("GROUP_B").alias("category_zone"),
    )
    .orderBy(F.col("id").desc())
)
