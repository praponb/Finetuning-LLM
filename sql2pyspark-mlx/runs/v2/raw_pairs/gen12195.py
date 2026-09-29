from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.col("age").between(59, 62))
    .select("id", "city", "credit_limit", F.rank().over(w).alias("credit_limit_rank"))
    .orderBy("credit_limit")
)
