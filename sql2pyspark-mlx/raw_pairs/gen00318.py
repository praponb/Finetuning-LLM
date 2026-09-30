from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age")

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select("id", "city", "age", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "city", "age")
    .orderBy(F.col("id").desc())
)
