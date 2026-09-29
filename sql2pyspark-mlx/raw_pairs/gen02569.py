from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w3 = Window.partitionBy("dept_id").orderBy("job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id")

filtered = (
    df_employees
    .filter(F.col("salary") > 60000)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        F.trim("job_title").alias("clean_job_title"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_id", "job_title", "clean_job_title", "dedup_rank")
)

with_dims = (
    latest
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.dept_id",
        "s.job_title",
        "s.clean_job_title",
        F.col("p.location"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("dept_id") == 6)
    .withColumn("job_title_length", F.length("job_title"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("N/A")))
)

with_windows = (
    cleaned
    .filter(F.col("job_title_length") >= 47)
    .select(
        "id",
        "dept_id",
        "clean_job_title",
        "job_title_clean",
        F.first("location", ignorenulls=True).over(w3).alias("first_known_location"),
        F.min("job_title_length").over(w4).alias("dept_id_min_job_title_length"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("dept_id_min_job_title_length") >= 63, "LARGE").otherwise("SMALL").alias("dept_id_min_job_title_length_tier"),
    )
    .agg(
        F.round(F.avg("dept_id_min_job_title_length"), 2).alias("avg_dept_id_min_job_title_length"),
        F.percentile_approx("dept_id_min_job_title_length", [0.25, 0.5, 0.75]).alias("dept_id_min_job_title_length_quartiles"),
    )
)
