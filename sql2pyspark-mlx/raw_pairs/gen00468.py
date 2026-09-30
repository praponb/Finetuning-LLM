from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select("segment")
    .intersect(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("job_title")
    )
    .orderBy("segment")
)
