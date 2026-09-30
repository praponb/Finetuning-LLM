from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select("id", "dept_id", "salary", F.dense_rank().over(w).alias("dense_rnk"))
)
