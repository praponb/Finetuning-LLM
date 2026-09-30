from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isNotNull())
    .select(F.col("country").alias("label"))
    .unionByName(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select(F.col("full_name").alias("label"))
    )
)
