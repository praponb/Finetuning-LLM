from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("salary").desc(), "id")

valid = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Finance"]))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

filtered = (
    valid
    .filter(F.col("budget").between(1893000, 2667000))
    .select(
        "*",
        (F.col("budget_filled") * F.col("budget")).alias("budget_filled_times_budget"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

latest = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Finance")))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_filled",
        "budget_filled_times_budget",
        "location_title",
        "rn",
        (F.col("budget_filled") * F.col("budget")).alias("budget_filled_times_budget_2"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

result = (
    base
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3197000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget_filled_times_budget")).otherwise(0)).alias("singapore_budget_filled_times_budget"),
        F.percentile_approx("budget_filled_times_budget_2", 0.5).alias("budget_filled_times_budget_2_p50"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy(F.col("budget_tier").desc())
)
