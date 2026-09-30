from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary")

result = (
    df_employees
    .select("id", "dept_id", "salary", "email", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "dept_id", "salary", "email")
)
