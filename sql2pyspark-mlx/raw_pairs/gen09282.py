from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy("budget", "id")

scored = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.max("budget").over(w_grp).alias("last3_max_budget"),
        F.count("budget").over(w_grp).alias("last3_count_budget"),
        F.min("budget").over(w_grp).alias("last3_min_budget"),
        F.lead("budget", 1).over(w_part).alias("next_budget"),
    )
)

kept_keys = (
    scored
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("dept_name").isin(["Engineering", "Marketing"]))
        .select("id", "dept_name")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("last3_max_budget").isNotNull())
    .select(
        "id",
        "dept_name",
        "last3_max_budget",
        "last3_min_budget",
        "next_budget",
        F.coalesce("last3_count_budget", F.lit(0)).alias("last3_count_budget_filled"),
        F.substring("dept_name", 1, 4).alias("dept_name_prefix"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([6, 9, 19, 20])), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("last3_count_budget_filled") >= 72, "PREMIUM").otherwise("STANDARD").alias("last3_count_budget_filled_tier"),
    )
    .agg(
        F.percentile_approx("last3_count_budget_filled", [0.25, 0.5, 0.75]).alias("last3_count_budget_filled_quartiles"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .orderBy(F.col("hr_count").asc_nulls_first())
)
