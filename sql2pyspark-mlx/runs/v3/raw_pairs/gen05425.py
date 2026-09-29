from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "amount", "discount")
    .orderBy(F.col("amount").desc(), "id")
)
