from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(F.col("city").alias("label"), F.lit("supplier").alias("origin"))
    .unionByName(
        df_departments
        .filter(F.col("location").isNotNull())
        .select(F.col("location").alias("label"), F.lit("department").alias("origin"))
    )
    .distinct()
    .orderBy("origin")
)
