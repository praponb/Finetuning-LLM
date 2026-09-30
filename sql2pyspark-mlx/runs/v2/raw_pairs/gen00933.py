from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")

result = (
    df_orders
    .select("id", "customer_id", "order_date", "amount", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
)
