from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w_grp = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w_grp_2 = Window.partitionBy("job_title").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.count("salary").over(w).alias("job_title_count_salary"),
        F.avg("salary").over(w_grp).alias("salary_avg_2"),
        F.ntile(3).over(w_grp_2).alias("salary_bucket"),
    )
)
