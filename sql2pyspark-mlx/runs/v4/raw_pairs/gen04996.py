from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("location").desc())
)
