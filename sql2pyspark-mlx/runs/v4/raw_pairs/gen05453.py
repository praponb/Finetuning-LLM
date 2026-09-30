from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("salary_rounded", "id")
w3 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("job_title").orderBy("next_salary_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("manager_id").orderBy("hire_date", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-01-05").cast("date")) & F.col("level").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 2))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

scored = (
    filtered
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_6_prior"),
        F.lead("salary_rounded", 1).over(w2).alias("next_salary_rounded"),
        F.avg("salary").over(w3).alias("salary_avg_3"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.hire_date").alias("employee_hire_date"))
)

enriched = (
    with_dims
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w5))
    .withColumn("salary_avg_3_avg_3_prior", F.avg("salary_avg_3").over(w6))
    .withColumn("first_known_salary_avg_3", F.first("salary_avg_3", ignorenulls=True).over(w7))
    .withColumn("next_salary_avg_6_prior", F.lead("salary_avg_6_prior", 1).over(w8))
)

result = (
    enriched
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "salary_rounded",
        F.when(F.col("level").rlike("^(L1|L2)$"), "PRIORITY").when(F.col("level").rlike("^(L3|L4)$"), "CORE").otherwise("EXTENDED").alias("level_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy("id")
)
