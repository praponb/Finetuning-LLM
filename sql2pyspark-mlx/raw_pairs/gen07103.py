from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .select("id", "dept_id", F.count("*").over(w).alias("dept_id_count"))
    .orderBy("dept_id_count", F.col("id").desc())
)
