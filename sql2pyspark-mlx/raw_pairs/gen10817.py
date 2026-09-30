from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")

scored = (
    df_employees
    .withColumn("prev_salary", F.lag("salary").over(w_ordered))
    .withColumn("next_salary", F.lead("salary").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("salary_bucket", F.ntile(3).over(w2))
)

result = (
    scored
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "is_remote",
        "next_salary",
        "row_seq",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
