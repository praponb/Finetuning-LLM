from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1900)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.dense_rank().over(w).alias("units_dense_rank"),
    )
)
