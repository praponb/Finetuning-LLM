from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isNotNull())
    .select("status")
    .unionByName(
        df_departments
        .filter(F.col("location").isNotNull())
        .select(F.col("location").alias("status"))
    )
    .orderBy(F.col("status").desc())
)
