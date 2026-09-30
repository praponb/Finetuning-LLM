from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("units") != 39)
    .select(
        "id",
        "store_id",
        "revenue",
        F.min("revenue").over(w).alias("store_id_min_revenue"),
    )
)
