from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select("id", "store_id", "quantity", F.ntile(4).over(w).alias("quantity_ntile"))
)
