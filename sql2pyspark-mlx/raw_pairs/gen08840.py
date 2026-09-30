from pyspark.sql import functions as F

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.is_remote")), "left_semi")
)

result = (
    not_flagged
    .cube(
        F.when(F.col("budget") >= 4124000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .orderBy(F.col("budget_stddev").desc())
)
