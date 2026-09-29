from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "product_name",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
)
