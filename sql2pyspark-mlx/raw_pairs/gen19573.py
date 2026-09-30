from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy(F.date_trunc("month", "last_restock"), "product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([5, 7, 12]))
    .select(
        "id",
        "product_id",
        "store_id",
        "quantity",
        "last_restock",
        F.rank().over(w).alias("rnk"),
        (F.col("quantity") - F.lag("quantity", 1, 0).over(w2)).alias("quantity_change"),
    )
)
