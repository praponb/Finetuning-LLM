from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Chaiyap%"))
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") >= "2024-08-01")
        .select("country")
    )
    .orderBy(F.col("country").desc())
)
