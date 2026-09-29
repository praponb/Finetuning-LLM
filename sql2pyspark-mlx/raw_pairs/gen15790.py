from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "henry ford")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "job_title", "hire_date")
)
