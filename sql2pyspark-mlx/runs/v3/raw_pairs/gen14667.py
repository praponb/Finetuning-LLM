from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("id", "level", "hire_date", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .orderBy("level", F.col("row_num").desc())
)
