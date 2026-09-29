from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2023-10-07")
    .select("country")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isin([27, 36]))
        .select("country")
    )
)
