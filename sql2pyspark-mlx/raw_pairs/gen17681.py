from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "customer_id", "channel", "order_date", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("customer_id").desc())
)
