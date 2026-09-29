from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-08-10").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("salary_int", F.col("salary").cast("int"))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
