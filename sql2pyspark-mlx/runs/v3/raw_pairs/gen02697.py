from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "category",
        "launch_date",
        F.last("category").over(w).alias("last_category"),
        F.ntile(10).over(w2).alias("launch_date_ntile"),
    )
)
