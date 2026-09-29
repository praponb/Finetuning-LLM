from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id").rowsBetween(-2, -1)

result = (
    df_inventory
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w_ordered))
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w2))
)
