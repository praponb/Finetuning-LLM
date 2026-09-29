from pyspark.sql import functions as F

result = (
    df_departments
    .select("location", F.lit("department").alias("record_type"))
    .union(
        df_stores
        .filter(F.col("opened_date").between("2023-04-01", "2025-07-01"))
        .select("city", F.lit("store").alias("record_type"))
    )
)
