from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .select("id", "country", "signup_date", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("id").desc())
)
