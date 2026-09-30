from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age")

result = (
    df_customers
    .select("id", "city", "age", "segment", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy("city")
)
