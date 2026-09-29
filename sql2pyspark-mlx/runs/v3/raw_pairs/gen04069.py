from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2024-11-09")
    .select("city")
    .intersect(
        df_customers
        .filter(F.col("segment").isin(["SMB", "Consumer"]))
        .select("city")
    )
)
