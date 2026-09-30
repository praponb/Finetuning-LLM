from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("salary").between(95000, 122000) & F.lower("email").like("%iam%"))
)

summary = (
    valid_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
)

result = summary.filter(F.col("pct_l5") >= 75)
