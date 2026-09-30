from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2024-10-01")
    .select(F.lit("store").alias("origin"), "region")
    .unionByName(
        df_employees
        .filter(F.col("is_remote"))
        .select(F.lit("employee").alias("origin"), F.col("level").alias("region"))
    )
)
