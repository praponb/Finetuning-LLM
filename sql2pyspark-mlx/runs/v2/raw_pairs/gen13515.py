from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.max("revenue").over(w).alias("store_id_max_revenue"),
    )
)
