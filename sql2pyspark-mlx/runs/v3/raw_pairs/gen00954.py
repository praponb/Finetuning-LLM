from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "stock_qty",
        "product_name",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "category", "stock_qty", "product_name")
    .orderBy(F.col("stock_qty").desc())
)
