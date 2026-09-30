from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount")

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Amazon", "Shell"]))
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "merchant", "amount")
    .orderBy("id")
)
