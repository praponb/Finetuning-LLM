from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-08-06").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.lower("level").alias("level_lower"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

valid_employees = (
    valid
    .filter(F.col("hire_date").between("2023-12-30", "2024-02-13"))
    .withColumn("salary_band", F.when(F.col("salary") >= 124000, "High").when(F.col("salary") >= 49000, "Medium").otherwise("Low"))
)

ranked = (
    valid_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "manager_id", "level_lower", "resolved_level", "salary_band")
)

matched_keys = (
    ranked
    .select("manager_id")
    .subtract(
        df_employees
        .filter(~F.col("is_remote"))
        .select("manager_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube("manager_id")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("manager_id").desc())
)
