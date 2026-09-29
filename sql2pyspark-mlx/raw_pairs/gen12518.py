from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("category", "brand").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "cost",
        "launch_date",
        "tags",
        F.row_number().over(w).alias("seq_no"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_3"),
    )
)
