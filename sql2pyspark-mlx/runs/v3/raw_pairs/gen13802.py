from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "price",
        "supplier_id",
        F.min("price").over(w).alias("brand_min_price"),
        F.row_number().over(w2).alias("rn"),
    )
)
