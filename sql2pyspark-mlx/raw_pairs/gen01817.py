from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date_plus_45d"), "dept_id")
w2 = Window.orderBy("salary")
w3 = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date_plus_45d", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-07-03").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws("/", "full_name", "email").alias("full_name_email_key"),
        F.date_add("hire_date", 45).alias("hire_date_plus_45d"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "dept_id",
        "hire_date",
        "is_remote",
        "full_name_email_key",
        "hire_date_plus_45d",
        F.stddev_samp("salary").over(w).alias("hire_date_plus_45d_month_stddev_samp_salary"),
        F.percent_rank().over(w2).alias("salary_percent_rank"),
        F.ntile(4).over(w3).alias("salary_bucket"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.dept_name"))
)

kept_keys = (
    enriched
    .select("id", "dept_id")
    .subtract(
        df_employees
        .filter(F.year("hire_date") == 2025)
        .select("id", "dept_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("salary_bucket_tier", F.when(F.col("salary_bucket") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("hire_date_plus_45d_month_stddev_samp_salary_tier", F.when(F.col("hire_date_plus_45d_month_stddev_samp_salary") >= 139236, "HIGH").otherwise("LOW"))
    .cube("salary_bucket_tier", "hire_date_plus_45d_month_stddev_samp_salary_tier")
    .agg(
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("salary_percent_rank")).otherwise(0)).alias("hr_salary_percent_rank"),
        F.max(F.when(F.col("dept_name") == "Sales", F.col("seq_no"))).alias("sales_max_seq_no"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("salary_percent_rank")).otherwise(0)).alias("hr_salary_percent_rank_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("hire_date_plus_45d_month_stddev_samp_salary_tier").desc())
)
