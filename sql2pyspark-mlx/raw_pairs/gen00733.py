from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.orderBy(F.col("salary").desc())
w3 = Window.partitionBy("job_title", "dept_id").orderBy("salary")

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.date_trunc("year", "hire_date").alias("hire_date_period"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "is_remote",
        "level",
        "hire_date_period",
        F.ntile(10).over(w).alias("salary_bucket"),
        F.rank().over(w2).alias("salary_rank"),
        F.rank().over(w3).alias("salary_rank_2"),
    )
)
