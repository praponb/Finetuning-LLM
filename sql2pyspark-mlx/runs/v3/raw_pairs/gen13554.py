from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "US", "DE"]))
    .select("id", "city", "age", F.min("age").over(w).alias("city_min_age"))
    .orderBy("city", "city_min_age")
)
