from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
)
