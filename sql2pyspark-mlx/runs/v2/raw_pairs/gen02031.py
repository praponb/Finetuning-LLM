from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("stock_qty").isNull())
    .select(
        "id",
        "category",
        "launch_date",
        F.dense_rank().over(w).alias("launch_date_dense_rank"),
    )
    .orderBy("category", "launch_date_dense_rank")
)
