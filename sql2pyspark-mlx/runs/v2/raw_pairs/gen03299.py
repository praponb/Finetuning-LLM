from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("country").isin(["UK", "DE", "SG"]))
    .select("is_active", "segment")
    .orderBy(F.col("segment").desc())
)
