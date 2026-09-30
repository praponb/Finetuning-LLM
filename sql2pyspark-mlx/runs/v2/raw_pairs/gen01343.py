from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "age")
    .orderBy(F.col("age").desc())
)
