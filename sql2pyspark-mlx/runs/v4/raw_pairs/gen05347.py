from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w_hist = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .withColumn("product_id_max_quantity", F.max("quantity").over(w))
    .withColumn("quantity_bucket", F.ntile(10).over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w_hist))
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "product_id_max_quantity",
        "quantity_bucket",
    )
)
