from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units")

result = (
    df_store_sales
    .filter(F.col("units").between(19, 39))
    .select(
        "id",
        "product_id",
        "store_id",
        "units",
        F.rank().over(w).alias("units_rank"),
    )
)
