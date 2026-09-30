from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date_weekofyear").asc_nulls_first())
w2 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "dept_id").orderBy(F.col("hire_date_weekofyear").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("hire_date_weekofyear").asc_nulls_first())

filtered = (
    df_employees
    .filter(F.col("dept_id").isin([38, 40]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.weekofyear("hire_date").alias("hire_date_weekofyear"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

result = (
    filtered
    .filter(F.col("full_name").like("%Oliv%"))
    .select(
        "id",
        "job_title",
        "hire_date",
        "is_remote",
        "is_remote_label",
        "resolved_job_title",
        F.rank().over(w).alias("hire_date_weekofyear_rank"),
        F.ntile(3).over(w2).alias("hire_date_weekofyear_bucket"),
        F.dense_rank().over(w3).alias("hire_date_weekofyear_dense_rank"),
    )
)
