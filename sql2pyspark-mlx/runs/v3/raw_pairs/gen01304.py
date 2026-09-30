from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Chiang Mai", "New York"]))
    .select("id", "opened_date", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("city") == "New York")
        .select("id", "opened_date", "store_name")
    )
    .distinct()
    .orderBy("store_name")
)
