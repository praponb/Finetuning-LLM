from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date") > "2024-04-01")
    .select("id", "status", "shipping_fee", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "status", "shipping_fee")
)
