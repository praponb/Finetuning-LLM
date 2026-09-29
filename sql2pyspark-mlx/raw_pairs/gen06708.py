from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select("signup_date")
    .union(
        df_products
        .filter(F.col("launch_date").isNotNull())
        .select("launch_date")
    )
    .distinct()
)
