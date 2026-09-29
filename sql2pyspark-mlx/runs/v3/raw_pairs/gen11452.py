from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("country", "city")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit").between(10800, 14300))
        .select("country", "city")
    )
    .distinct()
)
