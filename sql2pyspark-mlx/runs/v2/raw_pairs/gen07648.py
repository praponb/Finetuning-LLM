from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "product_name",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "category", "launch_date", "product_name")
    .orderBy(F.col("id").desc())
)
