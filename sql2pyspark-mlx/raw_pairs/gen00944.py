from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-26"))) & (~F.col("level").eqNullSafe(F.lit("L2"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.upper("email").alias("email_upper"),
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label"),
        F.coalesce("level", F.lit("NONE")).alias("level_clean"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
