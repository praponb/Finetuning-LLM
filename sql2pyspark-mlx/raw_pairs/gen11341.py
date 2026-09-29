from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("row_count").desc())

deduped = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name")
)

filtered = (
    deduped
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_anti")
)

filtered_departments = (
    not_flagged
    .filter(F.col("dept_name") == "HR")
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.when(F.col("dept_name") == "HR", 1).otherwise(0).alias("is_hr"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

result = (
    filtered_departments
    .groupBy(
        "dept_name",
        F.when(F.col("is_hr") >= 74, "LARGE").otherwise("SMALL").alias("is_hr_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.count("*").alias("row_count"),
    )
    .withColumn("row_count_rank", F.rank().over(w2))
)
