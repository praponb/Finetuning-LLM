from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age")

result = (
    df_customers
    .select("id", "country", "age", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("id").desc())
)
