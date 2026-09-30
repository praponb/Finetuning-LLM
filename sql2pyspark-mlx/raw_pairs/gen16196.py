from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("brand") == "Stark")
    .select(
        "id",
        "brand",
        "stock_qty",
        "supplier_id",
        "launch_date",
        "cost",
        F.rank().over(w).alias("stock_qty_rank"),
        (F.col("cost") - F.lag("cost", 2, 0).over(w2)).alias("cost_change"),
    )
)
