from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
    .orderBy("name")
)
