from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("id", "dept_id", "salary", F.rank().over(w).alias("salary_rank"))
    .orderBy("salary_rank")
)
