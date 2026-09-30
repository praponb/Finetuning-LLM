from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w_recent = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Legal"))))
    .select(
        "*",
        F.concat("dept_name", F.lit(" - "), "location").alias("dept_name_location"),
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_clean"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    ranked
    .filter(F.col("budget").between(1215000, 3522000))
    .select("*", F.col("rank_in_group").cast("string").alias("rank_in_group_str"))
)

enriched = (
    base
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_upper",
        "dept_name_clean",
        "rank_in_group",
        F.sum("budget").over(w_recent).alias("running_sum_budget"),
        F.avg("budget").over(w_recent).alias("running_avg_budget"),
        F.min("budget").over(w_recent).alias("running_min_budget"),
        F.count("budget").over(w_recent).alias("running_count_budget"),
    )
)

result = (
    enriched
    .select(
        "id",
        "dept_name",
        "running_sum_budget",
        F.col("running_min_budget").alias("RUNNING_MIN_BUDGET"),
        "budget",
        "running_avg_budget",
    )
    .unpivot(
        ids=["id", "dept_name"],
        values=["running_sum_budget", "RUNNING_MIN_BUDGET", "budget", "running_avg_budget"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
