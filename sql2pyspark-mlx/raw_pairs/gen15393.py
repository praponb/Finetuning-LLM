from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("stock_qty", "id")
w2 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("stock_qty", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("last_restock").between("2023-06-01", "2024-06-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("double").alias("quantity_num"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.quantity_num",
        F.col("b.stock_qty"),
        F.col("b.price"),
        F.col("b.brand"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .filter(F.month(F.col("p2.opened_date")).isin([1, 6, 12]))
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity",
        "s.quantity_num",
        "s.stock_qty",
        "s.price",
        "s.brand",
        F.col("p2.sqft"),
    )
)

enriched_inventory = (
    enriched
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("running_sqft", F.sum("sqft").over(w2))
    .withColumn("price_avg_2", F.avg("price").over(w3))
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.store_name"), F.col("p.opened_date"))
)

result = (
    enriched_inventory_2
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_num",
        "stock_qty",
        "brand",
        "row_seq",
        "running_sqft",
        "store_name",
    )
)
