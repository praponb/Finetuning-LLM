from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-01-01", "2025-04-13") & ~F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
)
