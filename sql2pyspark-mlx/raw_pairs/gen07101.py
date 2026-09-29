from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-09-08").cast("date"))
    .withColumn("job_title_email_key", F.concat_ws("-", "job_title", "email"))
)

scored = (
    valid
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.avg("salary").over(w).alias("salary_avg_6_prior"),
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

with_windows = (
    scored
    .filter(F.col("salary").between(112000, 139000))
    .withColumn("running_sum_row_seq", F.sum("row_seq").over(w4))
    .withColumn("running_min_row_seq", F.min("row_seq").over(w4))
    .withColumn("running_avg_row_seq", F.avg("row_seq").over(w4))
)

kept_keys = (
    with_windows
    .select("dept_id", "level")
    .intersect(
        df_employees
        .filter(F.col("dept_id").isin([25, 28, 40]))
        .select("dept_id", "level")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("row_seq") == 3)
    .select(
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "level",
        "salary_share",
        "running_min_row_seq",
        "running_avg_row_seq",
        F.coalesce("running_sum_row_seq", F.lit(0)).alias("running_sum_row_seq_filled"),
        F.regexp_replace("full_name", " ", "").alias("full_name_slug"),
    )
)

result = (
    base
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["location", "job_title"], ["location"], []],
        F.col("p.location").alias("location"),
        F.col("e.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.salary_share")).alias("min_salary_share"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
