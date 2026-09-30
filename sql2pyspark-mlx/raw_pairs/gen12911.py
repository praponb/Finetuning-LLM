from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "email")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") >= 6600)
        .select("id", "email")
    )
    .orderBy(F.col("email").desc())
)
