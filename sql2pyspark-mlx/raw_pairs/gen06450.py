from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "revenue",
        F.cume_dist().over(w).alias("units_cume_dist"),
        F.row_number().over(w2).alias("seq_no"),
        F.min("revenue").over(w3).alias("store_id_min_revenue"),
    )
)
