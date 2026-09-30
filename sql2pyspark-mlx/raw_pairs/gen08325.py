from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

result = (
    df_employees
    .filter(F.col("full_name").like("%Pim%"))
    .select("id", "level", "salary", F.avg("salary").over(w).alias("level_avg_salary"))
    .orderBy(F.col("salary").desc())
)
