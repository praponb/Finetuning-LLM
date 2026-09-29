from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "price",
        "cost",
        "supplier_id",
        "category",
        "launch_date",
        "stock_qty",
        F.last("cost").over(w).alias("last_cost"),
        F.lag("stock_qty", 1).over(w2).alias("prev_stock_qty"),
    )
    .orderBy(F.col("price").desc())
)
