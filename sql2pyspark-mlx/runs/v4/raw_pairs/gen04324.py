from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_first(), "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.col("salary").cast("int").alias("salary_int"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_filled"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
