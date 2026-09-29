from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name").orderBy("budget", "id")

cleaned = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select(
        "*",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
    )
)

scored = (
    cleaned
    .filter(F.col("budget") <= 2160000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_dept_name_key",
        F.lag("budget").over(w_grp).alias("prev_budget"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

matched_keys = (
    scored
    .select("location")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Engineering")
        .select("location")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").isNotNull()), "left_anti")
)

result = (
    eligible
    .withColumn("prev_budget_tier", F.when(F.col("prev_budget") >= 3287000, "LARGE").otherwise("SMALL"))
    .withColumn("budget_tier", F.when(F.col("budget") >= 3092000, "LARGE").otherwise("SMALL"))
    .rollup("prev_budget_tier", "budget_tier")
    .agg(
        F.round(F.avg("prev_budget"), 2).alias("avg_prev_budget"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.grouping("prev_budget_tier").alias("grouping_id"),
    )
    .orderBy("finance_rows")
)
