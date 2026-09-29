from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("hire_date") >= "2024-11-07")
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "level", "hire_date", "full_name")
    .orderBy("full_name")
)
