from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") >= 68000)
    .select("id", "hire_date", F.row_number().over(w).alias("rn"))
)
