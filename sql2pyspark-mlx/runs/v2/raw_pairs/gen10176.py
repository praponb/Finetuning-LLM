from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category", "supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .filter(F.col("cost") >= 230)
    .select(
        "id",
        "brand",
        "stock_qty",
        "category",
        "supplier_id",
        "cost",
        "product_name",
        F.min("stock_qty").over(w).alias("brand_min_stock_qty"),
        F.last("product_name").over(w2).alias("last_product_name"),
    )
)
