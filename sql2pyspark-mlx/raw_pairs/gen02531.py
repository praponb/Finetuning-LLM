from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("email_length", "id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy("email_length", "id")
w3 = Window.orderBy("event_seq", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("job_title").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("prev_email_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

cleaned = (
    df_employees
    .filter(F.col("level").isin(["L3", "L2"]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        F.length("email").alias("email_length"),
    )
)

base = (
    cleaned
    .filter(F.col("manager_id").isNotNull() & (F.col("email_length") == 61))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "email_length",
        F.regexp_replace("full_name", " ", "_").alias("full_name_slug"),
        F.when(F.col("job_title") == "Intern", 1).otherwise(0).alias("is_intern"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lead("email_length").over(w).alias("next_email_length"),
        F.lag("email_length").over(w).alias("prev_email_length"),
        F.lead("is_intern").over(w).alias("next_is_intern"),
        F.lag("is_intern").over(w).alias("prev_is_intern"),
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("prev_is_intern").over(w3).alias("prev_is_intern_avg_6_prior"),
        F.sum("prev_is_intern").over(w4).alias("cumulative_prev_is_intern"),
        F.last("next_is_intern", ignorenulls=True).over(w5).alias("last_known_next_is_intern"),
    )
)

result = (
    scored
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "email_length",
        "full_name_slug",
        "is_intern",
        "next_email_length",
        "prev_email_length",
        F.when(F.col("full_name").rlike("^(David Lee |Mia Rossi)$"), "PRIORITY").otherwise("GROUP_A").alias("full_name_zone"),
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("prev_email_length").desc_nulls_last())
)
