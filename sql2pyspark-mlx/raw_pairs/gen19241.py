from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L3", "L2"]) & F.col("full_name").like("Pi%"))
    .select(
        "manager_id",
        "email",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)
