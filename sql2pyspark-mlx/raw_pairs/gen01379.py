from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .filter(F.col("full_name").like("%Kanya%"))
    .select("id", "level", "salary", F.dense_rank().over(w).alias("salary_dense_rank"))
)
