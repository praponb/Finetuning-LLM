from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("job_title")
w3 = Window.partitionBy("job_title").orderBy("salary", "id")

deduped = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.location"))
)

enriched_employees = (
    enriched
    .select(
        "*",
        F.min("salary").over(w2).alias("job_title_min_salary"),
        F.lag("salary", 1).over(w3).alias("prev_salary"),
    )
)

result = (
    enriched_employees
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "job_title_min_salary",
        "prev_salary",
        F.when(F.col("job_title").rlike("^(Director|Analyst)$"), "OTHER_ZONE").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "PRIORITY").otherwise("CORE").alias("job_title_zone"),
    )
    .orderBy(F.col("hire_date").desc_nulls_last())
)
