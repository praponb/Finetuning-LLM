from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select("id", "manager_id", "hire_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
)
