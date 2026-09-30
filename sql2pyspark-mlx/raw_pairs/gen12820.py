from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date")

latest = (
    df_orders
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = latest.select("id", "order_date", "discount", "shipping_fee", "rank_in_group")
