from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("is_remote_flag", "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("N/A")))
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
)
