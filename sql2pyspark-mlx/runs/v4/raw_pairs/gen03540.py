from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())
w_hist = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("employee_job_title").orderBy(F.col("is_bangkok").asc_nulls_first())
w6 = Window.partitionBy("employee_job_title")
w7 = Window.partitionBy("dept_name")
w8 = Window.partitionBy("employee_job_title", "dept_name")
w9 = Window.partitionBy("location", "dept_name").orderBy(F.col("location_stddev_samp_is_bangkok").desc_nulls_last())

cleaned = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Legal", "Sales"]))
    .select(
        "*",
        F.when(F.col("location") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)

enriched = (
    cleaned
    .withColumn("budget_dense_rank", F.dense_rank().over(w_running))
    .withColumn("location_stddev_samp_is_bangkok", F.stddev_samp("is_bangkok").over(w_hist))
    .withColumn("budget_bucket", F.ntile(5).over(w3))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

kept_keys = (
    with_latest
    .select("id", "dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("budget") >= 2284000)
        .select("id", "dept_name")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.dense_rank().over(w5).alias("is_bangkok_dense_rank"),
        F.stddev_samp("budget_dense_rank").over(w6).alias("employee_job_title_stddev_samp_budget_dense_rank"),
        F.round((F.col("budget_dense_rank") / F.sum("budget_dense_rank").over(w7)), 4).alias("budget_dense_rank_share"),
        F.count("is_bangkok").over(w8).alias("employee_job_title_count_is_bangkok"),
        F.percent_rank().over(w9).alias("location_stddev_samp_is_bangkok_percent_rank"),
    )
)

totals = (
    metrics
    .rollup("employee_job_title")
    .agg(
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.sum("budget").alias("total_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("budget_per_marketing"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("total_budget") < 0)
)

result = (
    totals
    .select(
        "employee_job_title",
        "legal_rows",
        "budget_per_marketing",
        "grouping_id",
        F.when(F.col("employee_job_title").rlike("^(Director|Engineer)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("employee_job_title_zone"),
    )
    .orderBy(F.col("grouping_id").asc_nulls_first())
)
