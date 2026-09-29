from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "segment", "signup_date", "email")
    .orderBy(F.col("id").desc(), F.col("email").desc())
)
