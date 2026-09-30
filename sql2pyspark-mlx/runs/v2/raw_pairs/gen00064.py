from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2024-09-01", "2025-02-25"))
    .select("city", F.lit("customer").alias("source"))
    .unionByName(
        df_stores
        .filter(F.col("opened_date") > "2024-02-27")
        .select("city", F.lit("store").alias("source"))
    )
    .orderBy(F.col("city").desc())
)
