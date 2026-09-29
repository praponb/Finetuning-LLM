from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("last_restock")).orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
