from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .select("id", "email", "dept_id", "salary", "hire_date", "is_remote", "level")
    .orderBy(F.col("id").asc_nulls_last())
)
