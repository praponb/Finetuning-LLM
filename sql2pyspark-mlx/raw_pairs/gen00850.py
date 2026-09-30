from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy("hire_date", "dept_id")
)
