from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
)
