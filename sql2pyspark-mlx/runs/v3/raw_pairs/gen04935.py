from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("customer_id").desc(), F.col("balance").desc())
)
