from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .select("id", "dept_id", "hire_date", "full_name", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("hire_date").desc(), "full_name")
)
