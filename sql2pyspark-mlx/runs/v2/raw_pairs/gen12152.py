from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.col("salary").between(98000, 123000))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "level", "salary", "manager_id")
)
