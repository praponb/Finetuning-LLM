from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(F.lit("employee").alias("kind"), "level")
    .union(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select(F.lit("store").alias("kind"), "store_name")
    )
)
