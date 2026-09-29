from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-07-15", "2025-10-05"))
    .withColumn("region_lower", F.lower("region"))
    .withColumn("region_length", F.length("region"))
)
