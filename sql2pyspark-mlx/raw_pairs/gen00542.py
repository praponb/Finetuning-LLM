from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_id").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("hire_date"), "level").orderBy("salary", "id")

valid = (
    df_employees
    .filter((F.col("dept_id") == 27) & (F.col("hire_date") > F.to_date(F.lit("2023-07-22"))))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("level_filled", F.coalesce("level", F.lit("N/A")))
)

with_windows = (
    valid
    .select(
        "id",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "level_filled",
        F.min("is_remote_flag").over(w_grp).alias("running_min_is_remote_flag"),
        F.count("is_remote_flag").over(w_grp).alias("running_count_is_remote_flag"),
        F.avg("is_remote_flag").over(w_grp).alias("running_avg_is_remote_flag"),
        F.lead("is_remote_flag", 2).over(w2).alias("next_is_remote_flag"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("hire_date").between("2023-09-01", "2024-06-11") & F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "salary",
        "is_remote",
        "level",
        "running_count_is_remote_flag",
        "running_avg_is_remote_flag",
        "next_is_remote_flag",
        F.round("running_avg_is_remote_flag", 1).alias("running_avg_is_remote_flag_rounded"),
    )
)

filtered = (
    cleaned
    .filter(F.col("salary").between(85000, 128000))
    .select(
        "id",
        "email",
        "salary",
        "is_remote",
        "running_avg_is_remote_flag",
        "next_is_remote_flag",
        F.ceil("next_is_remote_flag").alias("next_is_remote_flag_whole"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([16, 20, 22, 24]))
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "email",
        "salary",
        "is_remote",
        "running_avg_is_remote_flag",
        "next_is_remote_flag_whole",
    )
    .orderBy(F.col("salary").desc())
)
