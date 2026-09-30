from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select(F.col("full_name").alias("label"))
    .subtract(
        df_suppliers
        .filter(F.col("city").isNotNull())
        .select(F.col("city").alias("label"))
    )
)
