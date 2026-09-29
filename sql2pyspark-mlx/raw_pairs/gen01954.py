from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("dept_name").orderBy("salary_avg_6_prior", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("employee_level")

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.col("hire_date") >= F.lit("2023-07-13").cast("date")))
    .withColumn("clean_full_name", F.trim("full_name"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
    .withColumn("level_filled", F.coalesce("level", F.lit("DIRECT")))
)

joined = (
    valid
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.budget"), F.col("b.dept_name"), F.col("b.location"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id").eqNullSafe(F.col("e.id")), "inner")
    .select(
        "src.*",
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.level").alias("employee_level"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "salary",
        "level_filled",
        "budget",
        "dept_name",
        "location",
        "employee_full_name",
        "employee_level",
        F.avg("salary").over(w).alias("salary_avg_6_prior"),
        F.ntile(5).over(w2).alias("salary_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("location") == "Berlin")
    .withColumn("employee_full_name_title", F.initcap(F.trim("employee_full_name")))
    .withColumn("resolved_location", F.coalesce("location", F.lit("NONE")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "level_filled",
        "budget",
        "dept_name",
        "salary_bucket",
        "employee_full_name_title",
        F.avg("salary").over(w3).alias("salary_avg_2"),
        F.round((F.col("salary_avg_6_prior") / F.sum("salary_avg_6_prior").over(w4)), 4).alias("salary_avg_6_prior_share"),
    )
)

result = (
    with_windows
    .groupBy(
        "dept_name",
        F.when(F.col("salary_bucket") >= 7, "HIGH").otherwise("LOW").alias("salary_bucket_tier"),
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
