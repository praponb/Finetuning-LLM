from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id", "category")

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        "category",
        "supplier_id",
        "cost",
        F.first("category").over(w).alias("first_category"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
    .orderBy(F.col("id").desc(), F.col("brand").desc())
)
