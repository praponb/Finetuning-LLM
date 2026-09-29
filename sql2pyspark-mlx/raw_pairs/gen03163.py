from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date"), "level").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select("id", "hire_date", "level", F.row_number().over(w).alias("hire_date_row"))
    .orderBy(F.col("id").desc())
)
