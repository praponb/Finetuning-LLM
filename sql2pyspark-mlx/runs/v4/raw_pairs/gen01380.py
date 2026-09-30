from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull() & (F.col("level").isin(["L5", "L3", "L2"]) | (F.year("hire_date") == 2024)))
    .select(
        "id",
        "level",
        "dept_id",
        F.add_months("hire_date", 12).alias("hire_date_plus_months"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)
