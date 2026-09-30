from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.col("salary") < 94000)
    .select("id", "manager_id", "salary", "level", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "manager_id", "salary", "level")
    .orderBy("salary")
)
