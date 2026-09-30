from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "product_name",
        F.first("product_name").over(w).alias("first_product_name"),
    )
    .orderBy("brand")
)
