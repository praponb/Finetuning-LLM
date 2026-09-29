from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("email").desc())
)
