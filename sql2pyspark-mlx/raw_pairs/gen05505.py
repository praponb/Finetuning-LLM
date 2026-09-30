from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("email")
    .unionByName(
        df_stores
        .filter(F.col("city").isNotNull())
        .select(F.col("city").alias("email"))
    )
    .distinct()
)
