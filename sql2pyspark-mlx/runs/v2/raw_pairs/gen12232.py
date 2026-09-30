from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("country") == "US")
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
)
