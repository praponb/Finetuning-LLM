from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
)
