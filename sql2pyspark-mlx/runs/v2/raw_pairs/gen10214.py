from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
)
