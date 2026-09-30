from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.first("revenue").over(w).alias("first_revenue"),
        F.sum("units").over(w2).alias("store_id_sum_units"),
    )
    .orderBy("id")
)
