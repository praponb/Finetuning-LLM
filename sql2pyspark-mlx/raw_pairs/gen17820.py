from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L3", "L4", "L2"]))
    .select(
        "id",
        "full_name",
        "manager_id",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)
