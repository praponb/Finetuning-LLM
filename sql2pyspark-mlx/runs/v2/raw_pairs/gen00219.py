from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(~F.col("status").isin(["PENDING", "COMPLETED", "SHIPPED"]))
    .select("id", "shipping_fee", F.ntile(3).over(w).alias("shipping_fee_ntile"))
)
