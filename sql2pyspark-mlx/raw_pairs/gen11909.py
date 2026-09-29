from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "job_title").orderBy("is_berlin", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("location").orderBy("rank_in_group", "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_upper", F.upper("location"))
    .withColumn("is_berlin", F.when(F.col("location") == "Berlin", 1).otherwise(0))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"))
)

deduped = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_latest_departments = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"), F.col("c.salary"))
)

eligible = (
    with_latest_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNull()), "left_semi")
)

ranked = (
    eligible
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "location",
        "budget",
        "is_berlin",
        "job_title",
        "rank_in_group",
        "employee_job_title",
        "salary",
    )
    .orderBy("employee_job_title")
)
