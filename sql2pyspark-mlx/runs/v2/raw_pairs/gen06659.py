from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("salary").between(108000, 145000))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
)
