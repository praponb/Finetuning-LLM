from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("region").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("cumulative_quantity", "id")

with_dims = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.store_name"), F.col("p.region"), F.col("p.opened_date"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.product_name").like("Ru%"))
    .select("a.*", F.col("p.stock_qty"))
)

with_windows = (
    enriched
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "opened_date",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("quantity").over(w3).alias("running_quantity"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "opened_date", "cumulative_quantity", "running_quantity")
)

result = (
    deduped
    .select("id", "store_id", "opened_date", "cumulative_quantity", "running_quantity")
)
