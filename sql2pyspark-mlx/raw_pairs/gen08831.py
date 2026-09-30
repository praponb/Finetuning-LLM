from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.col("city") == "New York")
        .select("id", "segment")
    )
)
