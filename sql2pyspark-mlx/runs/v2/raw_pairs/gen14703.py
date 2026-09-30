from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("id", "dept_id", "salary", F.row_number().over(w).alias("row_num"))
)
