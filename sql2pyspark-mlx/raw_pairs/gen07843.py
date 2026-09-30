from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc())
w2 = Window.orderBy(F.col("quantity_scaled").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 1.15), 2))
)

result = (
    base
    .withColumn("quantity_rank", F.rank().over(w))
    .withColumn("quantity_scaled_bucket", F.ntile(3).over(w2))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w3))
)
