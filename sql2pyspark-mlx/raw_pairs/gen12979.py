from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2024-08-19")
        .select("country")
    )
    .distinct()
    .orderBy("country")
)
