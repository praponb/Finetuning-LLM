from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.level"))
)

result = (
    with_latest
    .cube("dept_name")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
    )
    .orderBy(F.col("marketing_rows").desc())
)
