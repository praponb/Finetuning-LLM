from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_inventory
    .select(
        "id",
        "last_restock",
        F.min("quantity").over(w_part).alias("last7_min_quantity"),
        F.sum("quantity").over(w_part).alias("last7_sum_quantity"),
        F.avg("quantity").over(w_part).alias("last7_avg_quantity"),
    )
)

result = (
    scored
    .select("id", "last7_min_quantity")
    .orderBy(F.col("last7_min_quantity").desc())
)
