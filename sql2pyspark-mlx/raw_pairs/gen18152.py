from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "last_restock")).orderBy("quantity", "id")
w2 = Window.partitionBy(F.year("last_restock")).orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("store_id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.sum("quantity").over(w_ordered).alias("store_id_sum_quantity"),
    )
)
