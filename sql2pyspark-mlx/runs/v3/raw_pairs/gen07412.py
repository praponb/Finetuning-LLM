from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc())
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "brand",
        "price",
        "supplier_id",
        "stock_qty",
        F.percent_rank().over(w).alias("price_pct_rank"),
        F.first("price").over(w2).alias("first_price"),
    )
)
