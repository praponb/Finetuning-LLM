from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Engineering"]))
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy("dept_name")
)
