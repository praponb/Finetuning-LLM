from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

base = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select("*", F.substring("dept_name", 1, 2).alias("dept_name_prefix"))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "location", "dept_name_prefix")
)
