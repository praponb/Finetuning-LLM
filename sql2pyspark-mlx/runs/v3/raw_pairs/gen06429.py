from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc())

result = (
    df_customers
    .select("id", "age", F.dense_rank().over(w).alias("age_dense_rank"))
    .orderBy(F.col("id").desc(), F.col("age_dense_rank").desc())
)
