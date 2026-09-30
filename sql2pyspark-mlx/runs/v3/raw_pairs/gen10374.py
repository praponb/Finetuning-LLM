from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "store_id", "shipping_fee", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "store_id", "shipping_fee")
)
