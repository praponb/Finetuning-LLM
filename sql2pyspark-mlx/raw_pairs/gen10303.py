from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("last_restock")).orderBy(F.col("last_restock").desc(), "id")
w2 = Window.orderBy("quantity")
w_part = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.row_number().over(w).alias("event_seq"),
        F.rank().over(w2).alias("quantity_rank"),
        F.row_number().over(w_part).alias("row_seq"),
        F.last("quantity", ignorenulls=True).over(w4).alias("last_known_quantity"),
        F.avg("quantity").over(w_recent).alias("quantity_avg_3"),
    )
)
