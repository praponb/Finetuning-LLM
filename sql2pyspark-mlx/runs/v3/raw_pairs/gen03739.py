from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date")

result = (
    df_employees
    .select("id", "manager_id", "hire_date", F.rank().over(w).alias("hire_date_rank"))
)
