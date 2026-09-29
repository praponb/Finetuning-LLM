from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w_grp = Window.partitionBy(F.year("hire_date"), "job_title")
w3 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "level")
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "salary",
        F.ntile(10).over(w).alias("salary_bucket"),
        F.count("salary").over(w_grp).alias("hire_date_year_count_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.max("salary").over(w4).alias("manager_id_max_salary"),
        F.first("job_title", ignorenulls=True).over(w5).alias("first_known_job_title"),
    )
)
