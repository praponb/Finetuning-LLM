from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%FRAN%"))
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.month("hire_date").alias("hire_month"),
        "level",
    )
    .agg(F.min("hire_date").alias("first_hire_date"))
)
