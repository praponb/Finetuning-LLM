from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("hire_date") >= "2025-04-01")
    .select("id", "level", "hire_date", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy("rn")
)
