from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty")
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("launch_date") >= "2025-09-01")
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_id",
        "cost",
        F.percent_rank().over(w).alias("stock_qty_pct_rank"),
        F.row_number().over(w2).alias("rn"),
    )
)
