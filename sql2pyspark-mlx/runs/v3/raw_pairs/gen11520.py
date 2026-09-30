from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("category").orderBy("price", "id")

result = (
    df_products
    .filter(~F.col("category").isin(["Books", "Home"]))
    .select(
        "id",
        "category",
        "price",
        "supplier_id",
        F.last("supplier_id").over(w).alias("last_supplier_id"),
        F.ntile(10).over(w2).alias("bucket"),
    )
)
