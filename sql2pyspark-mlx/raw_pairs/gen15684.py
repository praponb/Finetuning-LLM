from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("min_age_by_country", F.min("age").over(w))
    .filter(F.col("age") == F.col("min_age_by_country"))
    .select("id", "country", "age", "city")
    .orderBy("country", "id")
)
