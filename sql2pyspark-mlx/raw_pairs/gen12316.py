from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date_month_end", "id")
w2 = Window.partitionBy("job_title").orderBy("salary")
w3 = Window.partitionBy("dept_id")

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-03-19").cast("date")) & F.col("level").isNotNull())
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
)

result = (
    base
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w2).alias("salary_cume_dist"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)
