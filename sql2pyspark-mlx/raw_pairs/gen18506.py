from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "country", "age")
)
