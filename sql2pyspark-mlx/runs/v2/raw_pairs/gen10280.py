from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2025-10-06")
        .select("country")
    )
    .distinct()
)
