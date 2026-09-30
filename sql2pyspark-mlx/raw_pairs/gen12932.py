from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .withColumn("running_min_quantity", F.min("quantity").over(w_part))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w_part))
    .withColumn("running_max_quantity", F.max("quantity").over(w_part))
    .withColumn("row_seq", F.row_number().over(w2))
)
