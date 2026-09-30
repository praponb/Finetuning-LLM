from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 5)
)
