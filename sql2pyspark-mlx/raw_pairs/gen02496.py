from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("rn").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy("salary", "id")
w4 = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc())

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-05").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "days_since_hire_date",
        "rn",
    )
)

deduped_employees = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped_employees
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "full_name", "hire_date", "rn", "latest_rank")
)

result = (
    latest
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
    )
    .filter(F.col("event_count") >= 1)
)
