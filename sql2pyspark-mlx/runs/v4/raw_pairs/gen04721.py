from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary_whole", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id")

base = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .withColumn("full_name_prefix", F.substring("full_name", 1, 2))
    .withColumn("level_filled", F.coalesce("level", F.lit("OTHER")))
)

joined = (
    base
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.email").alias("employee_email"),
        F.col("d.salary").alias("employee_salary"),
    )
)

valid = (
    joined
    .filter(F.col("employee_salary") > 73000)
    .withColumn("employee_email_lower", F.lower("employee_email"))
    .withColumn("salary_whole", F.ceil("salary"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("NONE")))
)

metrics = (
    valid
    .withColumn("salary_avg_3", F.avg("salary").over(w))
    .withColumn("salary_bucket", F.ntile(10).over(w2))
    .withColumn("salary_bucket_2", F.ntile(3).over(w3))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("salary_whole") >= 76, "LARGE").otherwise("SMALL").alias("salary_whole_tier"),
    )
    .agg(
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.round(F.avg("salary_bucket_2"), 2).alias("avg_salary_bucket_2"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("level").alias("distinct_levels"),
    )
    .filter(F.col("distinct_levels") >= 1)
    .orderBy("salary_p95")
)
