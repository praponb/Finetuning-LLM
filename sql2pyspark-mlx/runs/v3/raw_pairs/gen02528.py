from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .filter(F.col("customer_id") == 1)
    .select("id", "channel", "order_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
)
