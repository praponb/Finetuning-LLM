from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select(
        "id",
        "supplier_id",
        "price",
        "brand",
        "stock_qty",
        F.max("price").over(w).alias("supplier_id_max_price"),
        F.last("price").over(w2).alias("last_price"),
    )
    .orderBy(F.col("id").desc(), "stock_qty")
)
