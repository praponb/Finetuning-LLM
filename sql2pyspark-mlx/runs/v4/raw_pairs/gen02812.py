from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
        F.sum("quantity").over(w2).alias("product_id_sum_quantity"),
        F.sum("quantity").over(w3).alias("running_quantity"),
    )
)
