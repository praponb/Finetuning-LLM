from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")

result = (
    df_customers
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 1)
)
