from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "job_title", "salary", "email")
    .orderBy(F.col("id").desc())
)
