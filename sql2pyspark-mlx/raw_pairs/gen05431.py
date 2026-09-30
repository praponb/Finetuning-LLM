from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([22, 33]))
    .select("id", "level", "hire_date", F.row_number().over(w).alias("row_num"))
)
