from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-07-20").cast("date"))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lower("job_title").alias("job_title_lower"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_filled"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
