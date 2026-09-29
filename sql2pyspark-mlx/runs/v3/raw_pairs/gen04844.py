from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .select("id", "segment", "signup_date", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "segment", "signup_date")
)
