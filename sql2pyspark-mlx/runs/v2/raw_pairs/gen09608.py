from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.col("name").like("Da%"))
    .select("id", "country", "age", F.max("age").over(w).alias("country_max_age"))
)
