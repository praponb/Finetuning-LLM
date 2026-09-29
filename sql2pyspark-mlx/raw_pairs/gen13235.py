from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_band", F.when(F.col("budget") >= 3788000, "High").when(F.col("budget") >= 2052000, "Medium").otherwise("Low"))
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.row_num",
        F.col("c.level").alias("employee_level"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.row_num", "s.employee_level", F.col("c.salary"))
)

latest_match_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.row_num", "s.salary", F.col("c.salary").alias("employee_salary"))
)

result = (
    latest_match_departments
    .select("id")
    .orderBy(F.col("id").asc_nulls_first())
    .limit(5)
)
