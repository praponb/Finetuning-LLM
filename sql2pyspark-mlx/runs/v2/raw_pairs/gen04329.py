from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.month("hire_date").isin([1, 3, 5]))
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "level", "salary", "is_remote")
    .orderBy("level")
)
