from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        F.avg("quantity").over(w).alias("quantity_avg_3"),
        F.ntile(4).over(w_grp).alias("quantity_bucket"),
        F.ntile(10).over(w3).alias("quantity_bucket_2"),
        F.lead("quantity", 2).over(w4).alias("next_quantity"),
    )
)
