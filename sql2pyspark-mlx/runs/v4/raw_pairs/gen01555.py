from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_group", F.when(F.col("location").isin(["London", "Singapore"]), "Tier 1").otherwise("Other"))
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)
