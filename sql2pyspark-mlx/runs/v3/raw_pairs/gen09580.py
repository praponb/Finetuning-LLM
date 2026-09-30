from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([34, 38, 39]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "manager_id", "salary", "is_remote")
    .orderBy(F.col("manager_id").desc())
)
