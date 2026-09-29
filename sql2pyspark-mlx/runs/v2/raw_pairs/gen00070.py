from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")

result = (
    df_customers
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "city", "credit_limit")
)
