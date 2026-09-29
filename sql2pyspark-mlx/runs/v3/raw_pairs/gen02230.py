from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "dept_id", "hire_date", "level")
    .orderBy("id", "level")
)
