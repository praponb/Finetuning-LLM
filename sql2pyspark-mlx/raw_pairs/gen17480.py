from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
        F.row_number().over(w).alias("seq_no"),
    )
)
