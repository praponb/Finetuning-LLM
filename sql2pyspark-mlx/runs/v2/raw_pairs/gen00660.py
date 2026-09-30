from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")

result = (
    df_employees
    .select("id", "hire_date", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("id").desc(), "hire_date")
)
