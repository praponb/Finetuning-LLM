from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("status").desc())
)
