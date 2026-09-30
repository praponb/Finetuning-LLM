from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("dept_name").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy("salary", "id")
w5 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & ~F.col("location").isin(["Singapore", "Chiang Mai"]))
    .withColumn("location_prefix", F.substring("location", 1, 4))
    .withColumn("budget_rounded", F.round("budget", 1))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.budget", "s.location_prefix", F.col("c.salary"))
)

scored = (
    latest_match
    .select(
        "*",
        F.first("budget", ignorenulls=True).over(w2).alias("first_known_budget"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

valid = (
    scored
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "budget",
        "location_prefix",
        "salary",
        "first_known_budget",
        "row_seq",
        F.round((F.col("first_known_budget") * 100), 2).alias("adjusted_first_known_budget"),
    )
)

result = (
    valid
    .select(
        "id",
        "budget",
        "location_prefix",
        "first_known_budget",
        "row_seq",
        "adjusted_first_known_budget",
        F.avg("adjusted_first_known_budget").over(w5).alias("running_avg_adjusted_first_known_budget"),
        F.sum("adjusted_first_known_budget").over(w5).alias("running_sum_adjusted_first_known_budget"),
        F.count("adjusted_first_known_budget").over(w5).alias("running_count_adjusted_first_known_budget"),
        F.min("adjusted_first_known_budget").over(w5).alias("running_min_adjusted_first_known_budget"),
    )
)
