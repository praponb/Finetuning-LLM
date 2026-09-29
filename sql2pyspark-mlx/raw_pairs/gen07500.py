from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("dept_id")
w3 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w4 = Window.orderBy(F.col("row_seq").asc_nulls_first())

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-12").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.concat("email", F.lit(", "), "level").alias("email_level"),
        F.when(F.col("salary") >= 143000, "High").when(F.col("salary") >= 71000, "Medium").otherwise("Low").alias("salary_level"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "email_level",
        "salary_level",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

kept_keys = (
    metrics
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("salary").between(79000, 144000))
        .select("job_title")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    reconciled
    .select("dept_id")
    .subtract(
        df_employees
        .filter(F.col("is_remote"))
        .select("dept_id")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(~F.col("email").like("%@company.co"))
    .select(
        "id",
        "email",
        "job_title",
        "salary_level",
        "salary_share",
        F.dense_rank().over(w3).alias("salary_dense_rank"),
        F.rank().over(w4).alias("row_seq_rank"),
    )
)

result = (
    enriched
    .select("id", "email", "salary_level", "salary_dense_rank", "row_seq_rank")
    .orderBy(F.col("row_seq_rank").desc_nulls_first())
)
