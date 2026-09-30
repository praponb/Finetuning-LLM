from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .select("id", "country", "age", F.sum("age").over(w).alias("country_sum_age"))
    .orderBy("country_sum_age")
)
