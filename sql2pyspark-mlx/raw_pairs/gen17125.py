from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("supplier_name")
    .subtract(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select(F.col("job_title").alias("supplier_name"))
    )
)
