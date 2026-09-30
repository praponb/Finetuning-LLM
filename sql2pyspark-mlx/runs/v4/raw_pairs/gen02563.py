from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "supplier_id",
        "cost",
        "launch_date",
        "stock_qty",
        F.last("launch_date").over(w).alias("last_launch_date"),
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
    )
)
