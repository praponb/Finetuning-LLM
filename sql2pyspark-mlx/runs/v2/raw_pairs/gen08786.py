from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_orders
    .select("id", "status", "amount", "store_id", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("rnk").desc())
)
