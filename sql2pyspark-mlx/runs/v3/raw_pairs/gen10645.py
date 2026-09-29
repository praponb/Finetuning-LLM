from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "merchant", "amount")
)
