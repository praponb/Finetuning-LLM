from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") > 33)
    .select("id", "channel", "order_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "channel", "order_date")
)
