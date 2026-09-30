from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.lower("product_name").like("%ate%"))
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        "supplier_id",
        "price",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
        F.ntile(10).over(w2).alias("bucket"),
    )
)
