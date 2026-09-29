from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

result = (
    with_latest
    .select(
        "id",
        "location",
        "budget",
        "employee_salary",
        F.when(F.col("location").rlike("^(Chiang Mai|Singapore)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|New York)$"), "CORE").otherwise("EXTENDED").alias("location_zone"),
    )
    .orderBy(F.col("employee_salary").desc_nulls_first())
)
