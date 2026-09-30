from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("salary", "id")
w_part = Window.partitionBy("dept_name").orderBy(F.col("hire_date").desc(), "id")
w_grp = Window.partitionBy("dept_name").orderBy("hire_date", "id")

with_windows = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.last("job_title", ignorenulls=True).over(w).alias("last_known_job_title"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Engineering", "Finance"]))
    .select("a.id", "a.dept_id", "a.hire_date", "a.is_remote", F.col("d.dept_name"))
)

enriched_employees = (
    enriched
    .select(
        "id",
        "dept_id",
        "hire_date",
        "dept_name",
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

base = (
    enriched_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-17").cast("date"))
    .select(
        "id",
        "hire_date",
        "seq_no",
        F.when(F.col("dept_name") == "Sales", 1).otherwise(0).alias("is_sales"),
    )
)

stats = (
    base
    .groupBy(
        F.when(F.col("is_sales") >= 60, "PREMIUM").otherwise("STANDARD").alias("is_sales_tier"),
    )
    .agg(
        F.max("is_sales").alias("max_is_sales"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.percentile_approx("is_sales", [0.25, 0.5, 0.75]).alias("is_sales_quartiles"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
)

result = (
    stats
    .select("is_sales_tier", "avg_seq_no", "is_sales_quartiles")
    .orderBy(F.col("is_sales_tier").desc_nulls_last(), "avg_seq_no")
    .limit(10)
)
