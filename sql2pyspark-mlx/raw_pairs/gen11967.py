from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("job_title_count_salary").asc_nulls_first())
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("employee_job_title")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-11"))) & F.col("manager_id").isNotNull())
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("level_clean", F.coalesce("level", F.lit("NONE")))
)

enriched = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        F.count("salary").over(w).alias("job_title_count_salary"),
        F.avg("salary").over(w2).alias("salary_avg_2"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.*",
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.hire_date"),
    )
)

remaining_keys = (
    joined
    .select("job_title", "id")
    .exceptAll(
        df_employees
        .filter(~F.col("is_remote"))
        .select("job_title", "id")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(~F.col("is_remote"))
    .withColumn("job_title_count_salary_cume_dist", F.cume_dist().over(w3))
    .withColumn("salary_avg_2_share", F.round((F.col("salary_avg_2") / F.sum("salary_avg_2").over(w4)), 4))
    .withColumn("job_title_count_salary_share", F.round((F.col("job_title_count_salary") / F.sum("job_title_count_salary").over(w5)), 4))
)

by_type = (
    scored
    .groupBy("job_title")
    .pivot("employee_job_title", ["Engineer", "Director"])
    .agg(F.sum("salary_avg_2").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)

result = (
    by_type
    .select(
        "job_title",
        "Engineer_amount",
        "Engineer_item_count",
        "Director_amount",
        "Director_item_count",
    )
)
