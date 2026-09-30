from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        F.percent_rank().over(w).alias("stock_qty_pct_rank"),
    )
)
