from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "quantity",
        "product_id",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.percent_rank().over(w2).alias("last_restock_pct_rank"),
    )
    .orderBy(F.col("last_restock_pct_rank").desc())
)
