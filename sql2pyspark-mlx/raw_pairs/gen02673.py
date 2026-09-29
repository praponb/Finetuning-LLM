from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

scored = (
    df_employees
    .filter(F.col("hire_date") > "2024-12-01")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.max("salary").over(w).alias("last7_max_salary"),
        F.sum("salary").over(w).alias("last7_sum_salary"),
        F.count("salary").over(w).alias("last7_count_salary"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select("e.id", "e.dept_id", "e.manager_id", "e.hire_date", F.col("b.dept_name"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.dept_name").alias("department_dept_name"),
        F.col("p.location"),
        F.col("p.budget"),
    )
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "dept_id",
        "hire_date",
        "dept_name",
        "department_dept_name",
        "location",
    )
)

result = (
    ranked
    .groupBy(F.date_trunc("quarter", "hire_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.count(F.when(F.col("department_dept_name") == "Legal", 1)).alias("legal_rows"),
        F.count_if(F.col("department_dept_name") == "Legal").alias("legal_events"),
    )
    .orderBy(F.col("legal_events").desc_nulls_first())
)
