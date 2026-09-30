from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id")
w3 = Window.orderBy("salary", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-09-30").cast("date")) & F.col("job_title").isNotNull() & (F.col("salary") >= 96000))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

cleaned = (
    valid
    .filter((F.col("hire_date") > F.lit("2023-04-12").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "full_name", "job_title").alias("full_name_job_title_key"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "*",
        F.count("is_remote_flag").over(w2).alias("dept_id_count_is_remote_flag"),
        F.avg("is_remote_flag").over(w3).alias("is_remote_flag_avg_5_prior"),
        F.first("job_title", ignorenulls=True).over(w4).alias("first_known_job_title"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.dept_name"), F.col("d.budget"))
)

result = (
    with_dims
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "is_remote_flag",
        "full_name_job_title_key",
        "job_title_filled",
        "first_known_job_title",
    )
    .orderBy("is_remote_flag")
)
