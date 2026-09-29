from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment", "city")
    .intersect(
        df_customers
        .filter(F.col("is_active"))
        .select("segment", "city")
    )
)
