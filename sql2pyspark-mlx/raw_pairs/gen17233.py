from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("manager_id").isin([1, 13, 21]))
    .select("id", "dept_id", "salary", F.row_number().over(w).alias("row_num"))
)
