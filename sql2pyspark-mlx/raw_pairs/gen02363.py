from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("level", "dept_name").orderBy("salary", "id")
w3 = Window.partitionBy("dept_name")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("N/A")))
)

latest_match = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.dept_name_clean",
        F.col("c.level").alias("employee_level"),
        F.col("c.salary"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.salary", F.col("c.job_title"), F.col("c.level"))
)

valid = (
    with_last
    .filter(F.col("level") == "L1")
    .select(
        "id",
        "dept_name",
        "salary",
        "job_title",
        "level",
        F.concat_ws(",", "level", "dept_name").alias("level_dept_name_key"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lead("salary").over(w2).alias("next_salary"),
        F.row_number().over(w2).alias("seq_no"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level").isin(["L4", "L2", "L3"])), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("salary_share") >= 0.86, "LARGE").otherwise("SMALL").alias("salary_share_tier"),
    )
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("seq_no_per_l5"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("seq_no_p50") > 0)
)
