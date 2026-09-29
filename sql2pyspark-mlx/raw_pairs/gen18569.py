from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

valid = (
    df_employees
    .filter(F.col("salary") >= 65000)
    .withColumn("hire_date_plus_months", F.add_months("hire_date", 6))
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "full_name", "email", "dept_id", "manager_id", "is_remote", "level")
)
