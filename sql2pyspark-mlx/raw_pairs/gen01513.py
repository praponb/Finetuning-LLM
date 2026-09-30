from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

result = (
    df_employees
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
)
