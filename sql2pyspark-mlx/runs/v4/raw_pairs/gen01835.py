from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w3 = Window.orderBy(F.col("quantity").asc_nulls_last())
w4 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last())

scored = (
    df_inventory
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w))
    .withColumn("quantity_bucket", F.ntile(5).over(w2))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w3))
    .withColumn("running_quantity", F.sum("quantity").over(w4))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w5))
)

result = (
    scored
    .select(
        "id",
        "product_id",
        "quantity",
        "first_known_quantity",
        "quantity_bucket",
        "quantity_cume_dist",
        "quantity_percent_rank",
    )
    .orderBy(F.col("quantity").desc(), "id")
)
