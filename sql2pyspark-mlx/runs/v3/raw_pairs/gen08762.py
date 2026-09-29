from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "city", "credit_limit", "age")
)
