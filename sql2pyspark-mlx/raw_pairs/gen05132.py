from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location", "job_title").orderBy("employee_hire_date", "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.dept_name_lower",
        F.col("c.job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_last = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title").alias("employee_job_title"))
)

result = (
    with_last
    .groupBy(F.date_trunc("YEAR", "employee_hire_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
