from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("credit_limit").between(10800, 15700))
    .select("id", "city", "signup_date", F.rank().over(w).alias("signup_date_rank"))
    .orderBy("city")
)
