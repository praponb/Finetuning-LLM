from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("stock_qty").desc())
w2 = Window.partitionBy("category")

result = (
    df_products
    .filter(F.col("category") != "Home")
    .select(
        "id",
        "category",
        "supplier_id",
        "stock_qty",
        F.rank().over(w).alias("stock_qty_rank"),
        F.count("*").over(w2).alias("category_count"),
    )
    .orderBy("category", F.col("supplier_id").desc())
)
