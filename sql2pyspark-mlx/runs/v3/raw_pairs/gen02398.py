from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select("id", "country", "credit_limit", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "country", "credit_limit")
    .orderBy(F.col("credit_limit").desc())
)
