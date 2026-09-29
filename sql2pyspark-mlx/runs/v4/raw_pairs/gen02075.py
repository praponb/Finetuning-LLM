from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_whole").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4]))
    .withColumn("quantity_whole", F.ceil("quantity"))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

result = (
    filtered
    .withColumn("last_known_quantity_whole", F.last("quantity_whole", ignorenulls=True).over(w))
    .withColumn("next_quantity_whole", F.lead("quantity_whole", 2).over(w2))
    .withColumn("quantity_whole_bucket", F.ntile(5).over(w3))
)
