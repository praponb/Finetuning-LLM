from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "full_name",
        F.when(F.col("salary") >= 123000, "High").when(F.col("salary") >= 60000, "Medium").otherwise("Low").alias("salary_level"),
        F.date_add("hire_date", 51).alias("hire_date_plus_51d"),
    )
)
