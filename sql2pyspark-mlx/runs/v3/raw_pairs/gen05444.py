from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 5)
    .orderBy("signup_date")
)
