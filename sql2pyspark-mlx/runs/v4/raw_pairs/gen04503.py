from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w_grp = Window.orderBy("hire_date", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

enriched = (
    df_employees
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "event_seq",
        "seq_no",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("max_employee_salary").desc_nulls_first(), "id")
    .limit(20)
)
