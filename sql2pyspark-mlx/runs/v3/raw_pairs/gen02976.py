from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
)
