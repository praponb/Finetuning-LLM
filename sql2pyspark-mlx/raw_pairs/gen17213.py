from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select("id", "country", "credit_limit", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "country", "credit_limit")
    .orderBy("credit_limit")
)
