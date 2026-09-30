from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("city", "signup_date")
    .intersect(
        df_customers
        .filter(F.col("segment") == "Consumer")
        .select("city", "signup_date")
    )
)
