from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.orderBy(F.col("dept_name_length").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("dept_name").orderBy(F.col("dept_name_length_percent_rank").desc_nulls_first(), "id")

valid = (
    df_departments
    .filter(F.col("budget") <= 2733000)
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Marketing", "Finance"]), "Priority").otherwise("Other").alias("dept_name_group"),
    )
)

filtered = (
    valid
    .filter(F.col("budget") > 1117000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_group",
        F.length("dept_name").alias("dept_name_length"),
        F.trim("dept_name").alias("clean_dept_name"),
    )
)

with_windows = (
    filtered
    .filter(F.col("dept_name") == "Engineering")
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_group",
        "clean_dept_name",
        F.ntile(5).over(w).alias("budget_bucket"),
        F.last("dept_name", ignorenulls=True).over(w2).alias("last_known_dept_name"),
        F.row_number().over(w3).alias("seq_no"),
        F.percent_rank().over(w4).alias("dept_name_length_percent_rank"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.dept_name_group",
        "s.clean_dept_name",
        "s.dept_name_length_percent_rank",
        F.col("c.salary").alias("employee_salary"),
    )
)

latest = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

eligible = (
    latest
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") < 146000), "left_anti")
)

result = (
    eligible
    .cube("dept_name")
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("dept_name_length_percent_rank", [0.25, 0.5, 0.75]).alias("dept_name_length_percent_rank_quartiles"),
        F.sum("employee_salary").alias("total_employee_salary"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
    )
    .orderBy(F.col("num_dept_names").desc())
)
