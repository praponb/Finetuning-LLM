from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_orders
    .select("id", "status", "amount", F.rank().over(w).alias("rnk"))
    .orderBy("status")
)
