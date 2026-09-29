from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date")

result = (
    df_customers
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "segment", "signup_date")
)
