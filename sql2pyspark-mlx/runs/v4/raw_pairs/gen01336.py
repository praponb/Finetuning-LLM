from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title")
w2 = Window.partitionBy("level", "job_title").orderBy("salary", "id")
w_ordered = Window.partitionBy("level").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("job_title", "level")

filtered = (
    df_employees
    .filter(F.col("dept_id").isin([2, 10]))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
        F.concat_ws("-", "level", "job_title").alias("level_job_title_key"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.lag("salary", 2).over(w2).alias("prev_salary"),
        F.cume_dist().over(w_ordered).alias("salary_cume_dist"),
        F.stddev_samp("salary").over(w4).alias("job_title_stddev_samp_salary"),
    )
)
