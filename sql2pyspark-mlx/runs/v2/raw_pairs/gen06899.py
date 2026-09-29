from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "segment", "signup_date", "email")
    .orderBy("email")
)
