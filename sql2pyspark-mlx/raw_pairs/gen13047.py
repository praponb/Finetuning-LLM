from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "launch_date",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
)
