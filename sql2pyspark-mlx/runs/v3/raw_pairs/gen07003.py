from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .select("id", "status", "shipping_fee", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
