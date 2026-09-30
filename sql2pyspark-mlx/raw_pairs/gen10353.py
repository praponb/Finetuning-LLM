from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.length("product_name") >= 14)
    .select(
        "id",
        "brand",
        "launch_date",
        "category",
        "cost",
        F.row_number().over(w).alias("launch_date_row"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
    .orderBy(F.col("launch_date").desc())
)
