from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")

result = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
    )
)
