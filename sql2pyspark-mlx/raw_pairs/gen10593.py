from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.upper("email").like("%ENR%"))
    .select("id", "country", "age", F.avg("age").over(w).alias("country_avg_age"))
)
