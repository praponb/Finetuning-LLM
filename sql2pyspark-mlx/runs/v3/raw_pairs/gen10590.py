from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("discount").between(0.26, 0.33))
    .select("id", "status", "order_date", F.rank().over(w).alias("order_date_rank"))
)
