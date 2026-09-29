from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("employee_level").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("employee_level").orderBy(F.col("cumulative_budget_2").desc_nulls_first())
w7 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget_2").asc_nulls_last())
w8 = Window.partitionBy("employee_level")

deduped = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped_departments = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    deduped_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.level").alias("employee_level"),
    )
)

scored = (
    with_last
    .withColumn("cumulative_budget", F.sum("budget").over(w_hist))
    .withColumn("cumulative_budget_2", F.sum("budget").over(w5))
)

remaining_keys = (
    scored
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("location") == "Chiang Mai")
        .select("location", "dept_name")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

scored_departments = (
    verified
    .withColumn("cumulative_budget_2_dense_rank", F.dense_rank().over(w6))
    .withColumn("cumulative_budget_2_dense_rank_2", F.dense_rank().over(w7))
    .withColumn("cumulative_budget_share", F.round((F.col("cumulative_budget") / F.sum("cumulative_budget").over(w8)), 4))
)

result = (
    scored_departments
    .select(
        "dept_name",
        "location",
        "budget",
        "rn",
        "latest_rank",
        "employee_hire_date",
        "employee_level",
        "cumulative_budget_2",
        "cumulative_budget_2_dense_rank",
    )
    .orderBy(F.col("cumulative_budget_2").asc_nulls_first())
)
