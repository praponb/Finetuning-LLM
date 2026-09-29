from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "balance")
)
