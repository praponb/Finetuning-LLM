from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select(
        "id",
        "supplier_id",
        "price",
        "product_name",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 1)
    .select("id", "supplier_id", "price", "product_name")
)
