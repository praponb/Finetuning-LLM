from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("customer_id").isin([8, 10]))
    .select("id", "channel", "shipping_fee", F.row_number().over(w).alias("rn"))
)
