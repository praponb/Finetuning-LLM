from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
)
