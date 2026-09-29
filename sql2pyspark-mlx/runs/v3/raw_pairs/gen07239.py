from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "manager_id", "salary", "job_title")
    .orderBy(F.col("salary").desc())
)
