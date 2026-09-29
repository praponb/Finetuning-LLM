from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.col("signup_date") > "2023-08-08")
        .select("id", "segment")
    )
    .orderBy(F.col("segment").desc())
)
