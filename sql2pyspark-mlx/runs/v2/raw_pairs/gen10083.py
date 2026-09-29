from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .select("id", "city", "signup_date", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "city", "signup_date")
)
