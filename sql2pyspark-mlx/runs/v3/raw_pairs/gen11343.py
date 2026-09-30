from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_customers
    .filter(F.col("signup_date") <= "2023-03-01")
    .select("id", "city", "age", F.min("age").over(w).alias("city_min_age"))
    .orderBy(F.col("city_min_age").desc())
)
