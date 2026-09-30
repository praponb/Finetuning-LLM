from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        "last_restock",
        F.percent_rank().over(w).alias("quantity_pct_rank"),
        (F.col("quantity") - F.lag("quantity", 1).over(w2)).alias("quantity_change"),
    )
    .orderBy("product_id")
)
