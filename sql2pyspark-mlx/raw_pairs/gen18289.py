from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("job_title")
w3 = Window.partitionBy("manager_id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-08-22"))) & (F.length("email") > 12))
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.upper(F.trim("email")).alias("email_normalized"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

kept_keys = (
    cleaned
    .select("id", "level")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isin([3, 39]))
        .select("id", "level")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(~F.col("is_remote"))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("salary_share_2", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("job_title_max_salary", F.max("salary").over(w2))
)

scored = (
    enriched
    .select(
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "job_title_clean",
        "salary_share",
        "salary_share_2",
        F.count("job_title_max_salary").over(w2).alias("job_title_count_job_title_max_salary"),
        F.sum("salary_share").over(w3).alias("manager_id_sum_salary_share"),
    )
)

result = (
    scored
    .select(
        "full_name",
        "job_title",
        "level",
        "job_title_clean",
        "salary_share",
        "salary_share_2",
        "job_title_count_job_title_max_salary",
    )
    .orderBy(
        F.col("job_title_count_job_title_max_salary").desc(),
        F.col("job_title").desc(),
    )
)
