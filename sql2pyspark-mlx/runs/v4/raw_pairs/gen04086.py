from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary_bucket").desc(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-03-12")))
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("salary") >= 149000, "High").when(F.col("salary") >= 53000, "Medium").otherwise("Low").alias("salary_band"),
    )
)

with_windows = (
    valid
    .withColumn("salary_bucket", F.ntile(3).over(w))
    .withColumn("salary_avg_3", F.avg("salary").over(w2))
    .withColumn("salary_bucket_2", F.ntile(3).over(w3))
)

valid_employees = (
    with_windows
    .filter((F.col("hire_date") > F.to_date(F.lit("2024-02-27"))) & F.col("level").isNotNull() & F.col("is_remote"))
    .withColumn("job_title_level", F.concat("job_title", F.lit("/"), "level"))
)

latest = (
    valid_employees
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "salary_band",
        "salary_avg_3",
        "job_title_level",
        F.when(F.col("level").rlike("^(L5|L1)$"), "CORE").when(F.col("level").rlike("^(L2|L3)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("level_zone"),
        F.col("sum_employee_salary"),
    )
    .orderBy("dept_id")
)
