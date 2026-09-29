from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") <= 82000)
    .select("id", "dept_id", "hire_date", F.ntile(3).over(w).alias("bucket"))
    .orderBy(F.col("id").desc())
)
