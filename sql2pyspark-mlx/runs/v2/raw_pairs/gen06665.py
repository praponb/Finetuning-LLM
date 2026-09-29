from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("age") <= 55)
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "city", "age", "signup_date")
)
