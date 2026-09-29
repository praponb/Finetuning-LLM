from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "product_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "store_id", "units", "product_id")
    .orderBy("product_id")
)
