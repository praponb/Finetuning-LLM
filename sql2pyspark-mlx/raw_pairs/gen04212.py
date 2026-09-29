from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(~F.col("level").isin(["L1", "L4", "L5"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
)
