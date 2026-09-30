from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount")

result = (
    df_orders
    .select("id", "customer_id", "discount", "amount", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "customer_id", "discount", "amount")
)
