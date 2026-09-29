from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy("job_title")
)
