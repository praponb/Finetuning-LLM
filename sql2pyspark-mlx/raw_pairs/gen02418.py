from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w3 = Window.orderBy(F.col("salary").desc(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

enriched = (
    df_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.avg("salary").over(w).alias("salary_avg_6_prior"),
        F.lead("salary", 2).over(w2).alias("next_salary"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "salary_avg_6_prior",
        "next_salary",
        F.when(F.col("level").rlike("^(L1|L5)$"), "GROUP_B").when(F.col("level").rlike("^(L2|L3)$"), "CORE").otherwise("GROUP_A").alias("level_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("level_zone").desc())
)
