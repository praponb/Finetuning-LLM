from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
)
