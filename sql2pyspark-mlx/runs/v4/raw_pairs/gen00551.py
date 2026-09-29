from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_inventory
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("store_id_avg_quantity", F.avg("quantity").over(w2))
)
