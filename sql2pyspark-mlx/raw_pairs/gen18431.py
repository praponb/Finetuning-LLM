from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("adjusted_budget").desc(), "id")
w2 = Window.partitionBy("location")
w_part = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.orderBy("adjusted_budget_rounded", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("budget").between(1562000, 2632000))
    .select(
        "*",
        F.trim("dept_name").alias("clean_dept_name"),
        F.round((F.col("budget") * 1.15), 2).alias("adjusted_budget"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isNull()), "left_anti")
)

ranked = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    ranked
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
    )
)

valid = (
    filtered
    .filter(~F.col("location").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "location",
        "budget",
        "dept_name_location",
        F.round("adjusted_budget", 2).alias("adjusted_budget_rounded"),
        F.substring("location", 1, 3).alias("location_prefix"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

result = (
    valid
    .filter(F.col("budget").between(2530000, 3218000))
    .select(
        "id",
        "location",
        "budget",
        "dept_name_location",
        "adjusted_budget_rounded",
        "location_prefix",
        F.count("adjusted_budget_rounded").over(w2).alias("location_count_adjusted_budget_rounded"),
        F.sum("adjusted_budget_rounded").over(w_part).alias("cumulative_adjusted_budget_rounded"),
        F.avg("budget").over(w_part_2).alias("budget_avg_6"),
    )
)
