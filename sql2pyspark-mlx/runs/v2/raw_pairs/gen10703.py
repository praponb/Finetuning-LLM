from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(~F.col("city").isin(["Tokyo", "Singapore"]) & F.col("country").isin(["UK", "DE", "JP"]) & (F.col("country") == "US")))
    .agg(F.avg("rating").alias("avg_rating"))
    .orderBy(F.col("avg_rating").desc())
)
