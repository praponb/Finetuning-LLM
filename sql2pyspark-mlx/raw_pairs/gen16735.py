from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_id", "location").orderBy(F.col("budget").asc_nulls_first(), "id")

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-09-10")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.coalesce("full_name", F.lit("Unknown")).alias("full_name_filled"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_clean"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.email",
        "src.dept_id",
        "src.salary",
        "src.full_name_filled",
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_id", "full_name_filled", "budget", "location")
)

ranked = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.dept_id",
        F.col("d.location").alias("department_location"),
        F.col("d.budget").alias("department_budget"),
        F.col("d.dept_name"),
    )
)

totals = (
    enriched
    .withColumn("department_budget_tier", F.when(F.col("department_budget") >= 3364000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["department_budget_tier", "dept_id"], ["department_budget_tier"]],
        "department_budget_tier",
        "dept_id",
    )
    .agg(
        F.count(F.when(F.col("department_location") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
    )
)

result = totals.select("department_budget_tier", "dept_id", "tokyo_rows", "finance_rows")
