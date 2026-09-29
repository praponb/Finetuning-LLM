from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("stock_qty", "id").rowsBetween(-5, -1)
w4 = Window.orderBy(F.col("stock_qty").asc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy("stock_qty", "id")
w6 = Window.partitionBy("product_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"), F.col("p.stock_qty"))
)

scored = (
    with_dims
    .withColumn("first_known_category", F.first("category", ignorenulls=True).over(w))
    .withColumn("last_known_category", F.last("category", ignorenulls=True).over(w2))
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w3))
)

joined = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNull())
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.stock_qty",
        "e.first_known_category",
        F.col("p.product_name"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "stock_qty",
        F.dense_rank().over(w4).alias("stock_qty_dense_rank"),
        F.row_number().over(w5).alias("seq_no"),
        F.last("stock_qty", ignorenulls=True).over(w6).alias("last_known_stock_qty"),
    )
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
    )
)
