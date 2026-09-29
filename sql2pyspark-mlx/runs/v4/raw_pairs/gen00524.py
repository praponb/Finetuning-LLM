from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select("segment")
    .intersect(
        df_customers
        .filter(F.col("city").isin(["London", "Tokyo", "Chiang Mai"]))
        .select("segment")
    )
    .orderBy("segment")
)
