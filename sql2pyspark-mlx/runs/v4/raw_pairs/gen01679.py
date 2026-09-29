from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date")

result = (
    df_products
    .filter(F.col("category").isin(["Fashion", "Home"]))
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        F.dense_rank().over(w).alias("launch_date_dense_rank"),
    )
)
