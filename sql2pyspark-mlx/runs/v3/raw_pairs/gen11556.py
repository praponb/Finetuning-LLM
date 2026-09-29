from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2025-04-18")
    .select("id", "region", "city")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Berlin", "Chiang Mai"]))
        .select("id", "region", "city")
    )
    .distinct()
)
