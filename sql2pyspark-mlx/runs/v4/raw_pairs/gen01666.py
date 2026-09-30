from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("name").like("Iv%"))
    .select("id", "city", "signup_date", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("id").desc())
)
