from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Engineer", "Director"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "level", "hire_date")
    .orderBy(F.col("level").desc())
)
