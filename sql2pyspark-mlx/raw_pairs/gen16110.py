from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("dept_name", "level").orderBy("budget", "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title").alias("employee_job_title"))
)

with_windows = (
    with_last
    .withColumn("next_budget", F.lead("budget").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

kept_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_departments
        .filter(F.col("budget") >= 1660000)
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("next_budget") < 2251000)
    .select(
        "*",
        (F.col("budget") - F.col("next_budget")).alias("budget_minus_next_budget"),
        F.concat_ws("/", "employee_job_title", "location").alias("employee_job_title_location_key"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

remaining_keys = (
    base
    .select("location")
    .intersect(
        df_departments
        .filter(~F.col("dept_name").isin(["Engineering", "HR"]))
        .select("location")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "level",
        F.col("next_budget").alias("NEXT_BUDGET_VAL"),
        F.col("budget").alias("BUDGET_VAL"),
    )
    .unpivot(
        ids=["level"],
        values=["NEXT_BUDGET_VAL", "BUDGET_VAL"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
