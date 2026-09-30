from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("quantity", "id").rowsBetween(-6, -1)

with_windows = (
    df_inventory
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
)

result = (
    with_windows
    .select("id", "quantity", "last_restock", "quantity_avg_6_prior")
    .orderBy("quantity")
)
