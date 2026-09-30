from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "cost",
        "supplier_id",
        "category",
        "launch_date",
        "price",
        F.first("supplier_id").over(w).alias("first_supplier_id"),
        (F.col("price") - F.lag("price", 1).over(w2)).alias("price_change"),
    )
)
