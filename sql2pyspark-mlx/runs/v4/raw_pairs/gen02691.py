from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Singapore", "Tokyo"]))
    .select("id", "dept_name", F.count("*").over(w).alias("dept_name_count"))
    .orderBy("dept_name")
)
