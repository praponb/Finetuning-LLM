from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last())
w4 = Window.partitionBy("location").orderBy("first_known_budget_filled", "id")
w5 = Window.partitionBy("dept_name")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("budget").between(2271000, 2387000))
    .withColumn("location_filled", F.coalesce("location", F.lit("unassigned")))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("N/A")))
)

cleaned = (
    valid
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
    )
)

scored = (
    cleaned
    .withColumn("first_known_budget_filled", F.first("budget_filled", ignorenulls=True).over(w))
    .withColumn("cumulative_budget", F.sum("budget").over(w2))
    .withColumn("budget_percent_rank", F.percent_rank().over(w3))
)

cleaned_departments = (
    scored
    .filter(F.col("budget").between(2348000, 3317000))
    .withColumn("location_prefix", F.substring("location", 1, 4))
)

not_flagged = (
    cleaned_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([11, 26, 28])), "left_semi")
)

enriched = (
    not_flagged
    .filter(F.col("budget_percent_rank").between(0.77, 0.79))
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_clean",
        "dept_name_location",
        "budget_percent_rank",
        F.lag("budget_filled").over(w4).alias("prev_budget_filled"),
        F.lead("budget_filled").over(w4).alias("next_budget_filled"),
        F.lead("budget").over(w4).alias("next_budget"),
        F.row_number().over(w4).alias("seq_no"),
        F.min("cumulative_budget").over(w5).alias("dept_name_min_cumulative_budget"),
    )
)

result = (
    enriched
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_clean",
        "dept_name_location",
        "budget_percent_rank",
        "prev_budget_filled",
        "next_budget",
        "seq_no",
        F.when(F.col("dept_name").rlike("^(Finance|Sales)$"), "GROUP_B").when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "CORE").otherwise("PRIORITY").alias("dept_name_zone"),
    )
)
