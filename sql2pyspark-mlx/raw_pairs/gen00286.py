from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isNotNull())
    .select(F.col("country").alias("label"))
    .unionByName(
        df_employees
        .filter(F.col("level").isNotNull())
        .select(F.col("level").alias("label"))
    )
    .distinct()
)
