from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id")

result = (
    df_orders
    .select("id", "store_id", "shipping_fee", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "store_id", "shipping_fee")
    .orderBy(F.col("store_id").desc())
)
