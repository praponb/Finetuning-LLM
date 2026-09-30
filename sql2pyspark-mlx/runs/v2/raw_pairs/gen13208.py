from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.year("hire_date") >= 2025))
    .select(
        "id",
        "salary",
        F.date_sub("hire_date", 33).alias("hire_date_minus_33d"),
        F.trim("level").alias("clean_level"),
    )
)
