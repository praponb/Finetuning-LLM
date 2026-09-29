from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

enriched = (
    df_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.hire_date") < "2024-04-11")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.is_remote",
        "a.level",
        F.col("p.full_name").alias("employee_full_name"),
    )
)

cleaned = (
    enriched
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "manager_id",
        "job_title",
        "is_remote",
        F.concat_ws(",", "level", "job_title").alias("level_job_title_key"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_clean"),
    )
)

enriched_employees = (
    cleaned
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        F.col("p.level"),
        F.col("p.email"),
        F.col("p.salary"),
    )
)

cleaned_employees = (
    enriched_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("level_lower", F.lower("level"))
)

deduped = (
    cleaned_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .select("id", "manager_id", "email", "salary", "level_lower")
    .orderBy(F.col("manager_id").desc(), "salary")
)
