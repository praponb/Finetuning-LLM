from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount")

result = (
    df_orders
    .filter(F.col("store_id").isin([5, 34, 36]))
    .select("id", "store_id", "amount", "shipping_fee", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
)
