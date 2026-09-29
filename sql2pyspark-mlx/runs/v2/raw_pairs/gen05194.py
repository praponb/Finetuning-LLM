from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-04-09")
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        F.first("product_id").over(w).alias("first_product_id"),
    )
)
