from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([1, 17, 32, 33]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
)
