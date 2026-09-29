from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        "cost",
        F.first("cost").over(w).alias("first_cost"),
    )
)
