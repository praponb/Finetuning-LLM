from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("segment")
    .unionByName(
        df_customers
        .filter(F.col("country").isin(["JP", "DE"]))
        .select("segment")
    )
    .distinct()
)
