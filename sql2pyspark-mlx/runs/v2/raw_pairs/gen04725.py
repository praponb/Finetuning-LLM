from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

result = (
    df_employees
    .filter(F.col("hire_date") <= "2025-04-17")
    .select("id", "level", "salary", F.max("salary").over(w).alias("level_max_salary"))
    .orderBy("level_max_salary", "level")
)
