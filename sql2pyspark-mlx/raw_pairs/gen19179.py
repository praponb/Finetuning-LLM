from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("email_length").asc_nulls_last())
w3 = Window.partitionBy("dept_id").orderBy("email_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("email_length", "id")
w5 = Window.orderBy("email_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("email").like("%@yahoo.com"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
    )
)

valid = (
    cleaned
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "job_title_filled",
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
    )
)

valid_employees = (
    valid
    .filter(F.col("dept_id").isin([2, 14, 24]))
    .withColumn("email_length", F.length("email"))
)

enriched = (
    valid_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "job_title_filled",
        F.max("salary").over(w).alias("dept_id_max_salary"),
        F.cume_dist().over(w2).alias("email_length_cume_dist"),
        F.last("email_length", ignorenulls=True).over(w3).alias("last_known_email_length"),
        F.lag("email_length", 2).over(w4).alias("prev_email_length"),
        F.sum("salary").over(w5).alias("cumulative_salary"),
    )
)

remaining_keys = (
    enriched
    .select("dept_id", "id")
    .subtract(
        df_employees
        .filter(F.col("job_title") != "Manager")
        .select("dept_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "dept_id",
        "salary",
        "job_title_filled",
        "dept_id_max_salary",
        "last_known_email_length",
        "cumulative_salary",
    )
    .orderBy("cumulative_salary")
)
