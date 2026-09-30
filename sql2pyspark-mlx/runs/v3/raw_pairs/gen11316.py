from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 6000)
    .select("country")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isin([37, 40]))
        .select("country")
    )
)
