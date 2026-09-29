from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id")

with_windows = (
    df_inventory
    .withColumn("quantity_bucket", F.ntile(3).over(w))
    .withColumn("store_id_sum_quantity", F.sum("quantity").over(w2))
)

result = (
    with_windows
    .groupingSets(
        [["product_id", "store_id"], ["product_id"]],
        "product_id",
        "store_id",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.min("store_id_sum_quantity").alias("min_store_id_sum_quantity"),
    )
)
