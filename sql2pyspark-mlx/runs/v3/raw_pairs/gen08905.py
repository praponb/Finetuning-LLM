from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())

result = (
    df_orders
    .filter(F.col("store_id") == 9)
    .select("id", "customer_id", "discount", F.rank().over(w).alias("discount_rank"))
    .orderBy(F.col("id").desc())
)
