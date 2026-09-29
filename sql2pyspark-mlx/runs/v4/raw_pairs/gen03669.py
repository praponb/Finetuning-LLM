from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "*",
        F.count("revenue").over(w).alias("product_id_count_revenue"),
        F.lag("units", 1, 0).over(w2).alias("prev_units"),
    )
)
