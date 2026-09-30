from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select("id", "job_title", "salary", F.row_number().over(w).alias("row_num"))
)
