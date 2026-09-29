from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level", "manager_id").orderBy(F.col("hire_date").asc_nulls_first())
w3 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "full_name", "manager_id", "salary", "hire_date", "level")
)

with_dims = (
    deduped
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.*",
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.email"),
    )
)

deduped_employees = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "manager_id", "hire_date", "email")
)

valid = (
    deduped_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-30"))) & (F.col("hire_date") <= "2025-03-01"))
    .withColumn("email_upper", F.upper("email"))
)

joined = (
    valid
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.manager_id",
        "src.hire_date",
        "src.email",
        F.col("d.job_title"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "email",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("dept_id") == 23)
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "email", "seq_no", "event_seq")
    .orderBy(F.col("seq_no").desc_nulls_last())
)
