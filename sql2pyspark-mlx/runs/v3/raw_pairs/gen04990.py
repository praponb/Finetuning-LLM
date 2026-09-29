from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
    .orderBy(F.col("id").desc())
)
