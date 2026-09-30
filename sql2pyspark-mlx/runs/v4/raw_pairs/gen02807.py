from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "*",
        F.min("units").over(w).alias("store_id_min_units"),
        F.lag("units", 2).over(w2).alias("prev_units"),
    )
)
