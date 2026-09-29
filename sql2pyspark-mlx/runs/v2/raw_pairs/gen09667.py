from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "store_id",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 5)
    .select("id", "product_id", "revenue", "store_id")
)
