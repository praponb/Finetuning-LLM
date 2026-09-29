from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id").isNotNull() & F.col("customer_id").isin([12, 22, 26])) | F.col("browser").isNotNull())
    .select("id", "customer_id", "browser")
    .orderBy(F.col("id").desc())
    .limit(5)
)
