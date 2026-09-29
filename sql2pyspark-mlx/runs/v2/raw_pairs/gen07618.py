from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc())

result = (
    df_customers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "country", "age")
)
