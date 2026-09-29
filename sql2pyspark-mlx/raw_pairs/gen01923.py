from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "last_restock"), "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)

result = enriched.select("id", "first_known_quantity", "row_seq")
