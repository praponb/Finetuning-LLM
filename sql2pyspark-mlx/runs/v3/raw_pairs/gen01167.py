from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee")

result = (
    df_subscriptions
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 5)
)
