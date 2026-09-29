from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee").between(13, 26))
    .select("id", "store_id", "amount", F.ntile(10).over(w).alias("quartile"))
)
