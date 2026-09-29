from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "segment", "name")
    .unionByName(
        df_customers
        .filter(F.col("city") == "Chiang Mai")
        .select("id", "segment", "name")
    )
    .orderBy("segment")
)
