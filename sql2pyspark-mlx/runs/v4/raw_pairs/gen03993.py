from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "*",
        F.avg("units").over(w).alias("store_id_avg_units"),
        F.count("units").over(w2).alias("product_id_count_units"),
        F.last("units", ignorenulls=True).over(w3).alias("last_known_units"),
        F.lead("units", 1, 0).over(w4).alias("next_units"),
    )
)
