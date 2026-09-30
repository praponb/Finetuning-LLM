from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .select("location", F.lit("department").alias("source"))
    .union(
        df_customers
        .filter(F.col("city") == "Bangkok")
        .select("city", F.lit("customer").alias("source"))
    )
)
