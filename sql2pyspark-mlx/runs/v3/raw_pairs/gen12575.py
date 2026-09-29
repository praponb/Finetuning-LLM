from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(7244, 15637) & F.col("opened_date").between("2023-08-01", "2025-02-04"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.avg("sqft").alias("avg_sqft"))
)
