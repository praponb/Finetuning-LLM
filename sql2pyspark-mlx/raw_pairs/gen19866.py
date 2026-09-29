from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "hire_date"), "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w_recent = Window.partitionBy("job_title").orderBy("last_known_full_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-25").cast("date")) & (~F.col("level").eqNullSafe(F.lit("L1"))))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.trim("full_name").alias("clean_full_name"),
    )
)

cleaned = (
    filtered
    .filter((F.col("hire_date") >= F.lit("2023-03-21").cast("date")) & F.col("manager_id").isNotNull())
    .withColumn("full_name_length", F.length("full_name"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("UNKNOWN")))
)

enriched = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "full_name_length",
        F.last("full_name_length", ignorenulls=True).over(w).alias("last_known_full_name_length"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "full_name_length",
        "event_seq",
        F.avg("last_known_full_name_length").over(w_recent).alias("running_avg_last_known_full_name_length"),
        F.max("last_known_full_name_length").over(w_recent).alias("running_max_last_known_full_name_length"),
        F.count("last_known_full_name_length").over(w_recent).alias("running_count_last_known_full_name_length"),
    )
)

result = (
    scored
    .select(
        "id",
        "job_title",
        F.col("running_avg_last_known_full_name_length").alias("RUNNING_AVG_LAST_KNOWN_FULL_NAME_LENGTH_VAL"),
        "running_max_last_known_full_name_length",
    )
    .unpivot(
        ids=["id", "job_title"],
        values=["RUNNING_AVG_LAST_KNOWN_FULL_NAME_LENGTH_VAL", "running_max_last_known_full_name_length"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
