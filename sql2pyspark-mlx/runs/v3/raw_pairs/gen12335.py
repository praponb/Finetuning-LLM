from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .select("id", "hire_date", F.ntile(2).over(w).alias("quartile"))
)
