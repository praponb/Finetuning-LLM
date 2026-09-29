from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-2, -1)
w2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("budget", "id").rowsBetween(-3, -1)

filtered = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("manager_id") == 20))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "is_remote",
        "level",
        F.concat("email", F.lit("/"), "level").alias("email_level"),
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)

base = (
    filtered
    .filter(F.col("level").isNotNull() & F.col("level").isin(["L3", "L5"]))
    .select(
        "*",
        F.coalesce("level", F.lit("unassigned")).alias("level_filled"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.email",
        "s.dept_id",
        "s.is_remote",
        "s.level",
        "s.email_level",
        "s.level_clean",
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)

enriched = (
    with_dims
    .filter(F.col("dept_name") != "Sales")
    .select(
        "id",
        "is_remote",
        "location",
        "dept_name",
        "budget",
        F.avg("budget").over(w).alias("budget_avg_2_prior"),
        F.sum("budget").over(w2).alias("running_budget"),
        F.first("budget", ignorenulls=True).over(w3).alias("first_known_budget"),
        F.avg("budget").over(w4).alias("budget_avg_3_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "is_remote",
        "dept_name",
        "budget",
        "budget_avg_2_prior",
        "first_known_budget",
        "budget_avg_3_prior",
        F.when(F.col("dept_name").rlike("^(Legal|Finance)$"), "PRIORITY").otherwise("EXTENDED").alias("dept_name_zone"),
    )
    .orderBy(F.col("budget_avg_3_prior").desc())
)
