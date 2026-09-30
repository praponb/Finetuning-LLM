from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc())
w2 = Window.orderBy("launch_date")

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "launch_date",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.dense_rank().over(w2).alias("launch_date_dense_rank"),
    )
    .orderBy(F.col("supplier_id").desc())
)
