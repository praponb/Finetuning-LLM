from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date")

result = (
    df_employees
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "manager_id", "hire_date")
    .orderBy("hire_date", F.col("id").desc())
)
