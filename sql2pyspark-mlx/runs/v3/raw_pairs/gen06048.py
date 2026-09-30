from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("salary") < 125000)
    .select("id", "salary", F.row_number().over(w).alias("rn"))
)
