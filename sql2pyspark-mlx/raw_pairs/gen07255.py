from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment", "country")
    .intersect(
        df_customers
        .filter(F.col("signup_date") <= "2024-08-06")
        .select("segment", "country")
    )
)
