from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "supplier_id").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "cost",
        "launch_date",
        "stock_qty",
        F.min("cost").over(w).alias("supplier_id_min_cost"),
        F.dense_rank().over(w2).alias("stock_qty_dense_rank"),
    )
)
