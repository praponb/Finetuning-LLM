from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity")

result = (
    df_inventory
    .select("id", "store_id", "quantity", F.rank().over(w).alias("quantity_rank"))
)
