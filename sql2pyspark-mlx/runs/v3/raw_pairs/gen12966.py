from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("full_name").like("%bob%"))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
    .orderBy("full_name")
)
