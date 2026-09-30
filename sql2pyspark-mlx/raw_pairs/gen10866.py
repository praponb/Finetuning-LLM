from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .select("id", "level")
    .intersect(
        df_employees
        .filter(F.col("hire_date") > "2024-11-01")
        .select("id", "level")
    )
)
