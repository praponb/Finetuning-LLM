from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("city")
    )
)
