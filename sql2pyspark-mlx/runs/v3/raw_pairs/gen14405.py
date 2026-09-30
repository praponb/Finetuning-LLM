from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 31)
    .select("id", "amount", F.ntile(2).over(w).alias("amount_ntile"))
    .orderBy(F.col("amount_ntile").desc())
)
