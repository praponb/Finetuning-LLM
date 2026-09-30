from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select("id", "city", "age", F.dense_rank().over(w).alias("dense_rnk"))
)
