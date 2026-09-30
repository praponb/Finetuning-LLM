from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select("id", "city", "credit_limit", "name", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
)
