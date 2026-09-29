from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.partitionBy("job_title", "level").orderBy("salary", "id")
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy("rank_in_group", "id")
w6 = Window.partitionBy("level")
w7 = Window.partitionBy("dept_id").orderBy(F.col("salary_avg_3").desc(), "id")
w8 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("manager_id").orderBy("salary_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-25").cast("date")) & F.col("manager_id").isNotNull() & (F.col("job_title") == "Intern"))
    .withColumn("salary_band", F.when(F.col("salary") >= 125000, "High").when(F.col("salary") >= 73000, "Medium").otherwise("Low"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("NONE")))
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_3"),
        F.ntile(10).over(w_part).alias("salary_bucket"),
        F.last("job_title", ignorenulls=True).over(w3).alias("last_known_job_title"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .withColumn("salary_bucket_share", F.round((F.col("salary_bucket") / F.sum("salary_bucket").over(w6)), 4))
    .withColumn("salary_avg_3_bucket", F.ntile(5).over(w7))
    .withColumn("running_salary_avg_3", F.sum("salary_avg_3").over(w8))
    .withColumn("running_salary_avg_3_2", F.sum("salary_avg_3").over(w9))
)

joined = (
    enriched
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.dept_name"), F.col("p.location"), F.col("p.budget"))
)

cleaned = (
    joined
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "salary_band",
        "job_title_filled",
        "salary_avg_3",
        "salary_bucket",
        "last_known_job_title",
        "row_num",
        "salary_bucket_share",
        "running_salary_avg_3_2",
        "dept_name",
        "location",
        F.coalesce("salary_bucket_share", F.lit(0)).alias("salary_bucket_share_filled"),
    )
)

totals = (
    cleaned
    .groupBy("manager_id")
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.avg("salary_bucket_share").alias("avg_salary_bucket_share"),
        F.round(F.sum("salary_bucket_share") / F.nullif(F.count(F.when(F.col("job_title") == "Analyst", 1)), F.lit(0)), 2).alias("salary_bucket_share_per_analyst"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_rows"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary_bucket_share"))).alias("l5_total_salary_bucket_share"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
)

result = (
    totals
    .select("manager_id", "num_job_titles", "avg_salary_bucket_share")
    .orderBy(F.col("avg_salary_bucket_share").desc_nulls_last())
)
