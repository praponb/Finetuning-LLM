from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.sum("quantity").over(w_ordered).alias("running_sum_quantity"),
        F.avg("quantity").over(w_ordered).alias("running_avg_quantity"),
        F.count("quantity").over(w_ordered).alias("running_count_quantity"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
    )
)
