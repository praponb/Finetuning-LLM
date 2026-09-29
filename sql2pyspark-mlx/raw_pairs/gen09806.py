from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-06-01")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
)
