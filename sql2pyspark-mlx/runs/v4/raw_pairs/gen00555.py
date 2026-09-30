from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7, 8]))
    .select(
        "id",
        "product_id",
        "units",
        F.min("units").over(w).alias("product_id_min_units"),
    )
)
