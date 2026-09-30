from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%mma%") & ~F.col("email").like("%@company.co"))
    .select(
        "id",
        "email",
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label"),
    )
)
