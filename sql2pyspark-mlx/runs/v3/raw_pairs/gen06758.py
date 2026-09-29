from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.2, 4.2))
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2024-12-31")
        .select("country")
    )
)
