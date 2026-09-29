from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("manager_id", "dept_id").orderBy("prev_salary", "id")
w5 = Window.partitionBy("level").orderBy("salary", "id")

with_windows = (
    df_employees
    .select(
        "*",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.lag("salary", 1).over(w2).alias("prev_salary"),
        F.ntile(10).over(w3).alias("salary_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    deduped
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("manager_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"))
)

result = (
    enriched
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .groupingSets(
        [["location", "job_title"], ["location"], []],
        F.col("d.location").alias("location"),
        F.col("a.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.budget")).alias("max_budget"),
    )
)
