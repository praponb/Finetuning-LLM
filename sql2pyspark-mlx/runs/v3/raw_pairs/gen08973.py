from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(~F.col("full_name").like("%Park%"))
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 1)
    .select("id", "dept_id", "salary", "hire_date")
)
