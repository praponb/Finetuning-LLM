from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("level").orderBy(F.col("budget_int").desc_nulls_last(), "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.upper("location").alias("location_upper"),
    )
)

cleaned = (
    filtered
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "location",
        "budget",
        "dept_name_title",
        "location_upper",
        F.col("budget").cast("int").alias("budget_int"),
        F.floor("budget").alias("budget_whole"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.job_title"))
)

base = (
    latest_match
    .filter(F.col("job_title").isNotNull())
    .select("*", F.round((F.col("budget_int") * 0.9), 2).alias("adjusted_budget_int"))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    latest
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") <= 1259000)
        .select("id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "location",
        "budget",
        "dept_name_title",
        "budget_int",
        "budget_whole",
        "location_clean",
        "level",
        "job_title",
    )
    .orderBy(F.col("level").asc_nulls_first())
)
