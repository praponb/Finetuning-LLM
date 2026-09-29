from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name", "location").orderBy("salary", "id")
w4 = Window.partitionBy("dept_name").orderBy("employee_hire_date", "id")

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

with_last = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.hire_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        "s.hire_date",
        F.col("c.salary"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

deduped = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

eligible = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.job_title").isin(["Intern", "Manager"])), "left_semi")
)

ranked = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name", "location", "hire_date", "salary")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([17, 22, 29, 33])), "left_anti")
)

totals = (
    not_flagged
    .groupBy("dept_name")
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("salary")).otherwise(0)).alias("legal_salary"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.countDistinct("location").alias("distinct_locations"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
    )
    .filter(F.col("new_york_count") < 1000)
)

result = (
    totals
    .select(
        "dept_name",
        "new_york_count",
        "salary_p90",
        "distinct_locations",
        F.when(F.col("dept_name").rlike("^(HR|Marketing)$"), "GROUP_A").otherwise("CORE").alias("dept_name_zone"),
    )
)
