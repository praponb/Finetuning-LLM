from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.col("name").like("%bob%"))
    .select("id", "country", "age", F.min("age").over(w).alias("country_min_age"))
    .orderBy(F.col("country_min_age").desc())
)
