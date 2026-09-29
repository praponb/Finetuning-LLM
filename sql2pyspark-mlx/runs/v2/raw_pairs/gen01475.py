from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("age") >= 55)
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "segment", "signup_date", "age")
    .orderBy(F.col("age").desc(), F.col("signup_date").desc())
)
