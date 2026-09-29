from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("manager_id") == 18)
    .select("id", "manager_id", "salary", F.row_number().over(w).alias("row_num"))
)
