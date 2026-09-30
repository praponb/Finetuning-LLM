from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id").isNotNull() & (F.to_date("started_at") < "2023-03-11")) | (F.col("browser") == "Chrome"))
    .orderBy(F.col("country").desc())
)
