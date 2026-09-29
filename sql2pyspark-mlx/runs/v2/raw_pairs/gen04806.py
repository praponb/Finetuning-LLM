from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .select("id", "level", "hire_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "level", "hire_date")
)
