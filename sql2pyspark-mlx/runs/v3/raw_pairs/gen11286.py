from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .select(
        "id",
        "category",
        "supplier_id",
        "launch_date",
        F.dense_rank().over(w).alias("launch_date_dense_rank"),
    )
)
