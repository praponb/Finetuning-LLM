from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("next_stock_qty", "id")
w3 = Window.partitionBy("product_id").orderBy("prev_quantity", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity",
        F.col("b.stock_qty"),
    )
)

scored = (
    enriched
    .filter(F.col("product_id").isin([3, 5, 10]))
    .select(
        "*",
        F.lag("quantity").over(w_recent).alias("prev_quantity"),
        F.lead("quantity").over(w_recent).alias("next_quantity"),
        F.lag("stock_qty").over(w_recent).alias("prev_stock_qty"),
        F.lead("stock_qty").over(w_recent).alias("next_stock_qty"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

enriched_inventory = (
    scored
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("next_prev_stock_qty", F.lead("prev_stock_qty", 1).over(w3))
    .withColumn("running_prev_quantity", F.sum("prev_quantity").over(w4))
)

with_dims = (
    enriched_inventory
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.quantity",
        "e.prev_quantity",
        "e.next_quantity",
        "e.next_stock_qty",
        "e.next_prev_stock_qty",
        "e.running_prev_quantity",
        F.col("p.sqft"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "next_quantity",
        "next_stock_qty",
        "next_prev_stock_qty",
        "sqft",
    )
)
