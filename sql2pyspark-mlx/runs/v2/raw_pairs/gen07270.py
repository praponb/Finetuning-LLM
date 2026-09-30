from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select("id", "status", "shipping_fee", "discount", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "status", "shipping_fee", "discount")
)
