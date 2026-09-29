from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "manager_id", "salary", "level")
    .orderBy(F.col("salary").desc())
)
