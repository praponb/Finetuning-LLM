from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "customer_id", "amount", "shipping_fee")
)
