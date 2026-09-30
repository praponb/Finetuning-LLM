from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")

result = (
    df_customers
    .filter(F.col("credit_limit").between(7700, 9000))
    .select("id", "city", "country", "age", F.avg("age").over(w).alias("city_avg_age"))
)
