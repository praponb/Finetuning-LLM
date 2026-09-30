from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com"))
    .select("id", "city", "signup_date", F.rank().over(w).alias("rnk"))
    .orderBy("id")
)
