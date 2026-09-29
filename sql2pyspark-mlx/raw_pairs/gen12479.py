from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isNotNull())
    .select("country")
    .union(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("job_title")
    )
    .distinct()
)
