from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.lit("department").alias("source"))
    .unionByName(
        df_customers
        .select(F.col("segment").alias("location"), F.lit("customer").alias("source"))
    )
)
