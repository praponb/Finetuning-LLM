from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select("id", "credit_limit", "city", F.first("city").over(w).alias("first_city"))
    .orderBy(F.col("id").desc(), "first_city")
)
