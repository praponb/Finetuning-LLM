from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w_part = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("quantity", ignorenulls=True).over(w_part).alias("first_known_quantity"),
        F.avg("quantity").over(w3).alias("quantity_avg_5"),
    )
)
