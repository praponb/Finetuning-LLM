from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L5")
    .select(
        "is_remote",
        "email",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
    )
)
