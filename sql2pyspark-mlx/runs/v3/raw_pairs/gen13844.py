from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("order_date").between("2024-04-01", "2024-07-01"))
    .select("id", "channel", "order_date", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "channel", "order_date")
)
