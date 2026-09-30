from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "store_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .orderBy(F.col("product_id").desc())
)
