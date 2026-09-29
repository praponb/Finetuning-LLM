from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .select("id", "dept_id", "hire_date", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("dept_id").desc(), F.col("rnk").desc())
)
