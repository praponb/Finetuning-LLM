from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 41)
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([2, 3]))
        .select("country")
    )
)
