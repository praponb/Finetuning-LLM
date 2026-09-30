from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select(
        "id",
        "product_id",
        "units",
        F.dense_rank().over(w).alias("units_dense_rank"),
    )
    .orderBy(F.col("units_dense_rank").desc())
)
