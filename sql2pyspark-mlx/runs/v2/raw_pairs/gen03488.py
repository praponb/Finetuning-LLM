from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
)
