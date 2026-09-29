from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date")

result = (
    df_employees
    .select("id", "manager_id", "hire_date", "full_name", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "manager_id", "hire_date", "full_name")
)
