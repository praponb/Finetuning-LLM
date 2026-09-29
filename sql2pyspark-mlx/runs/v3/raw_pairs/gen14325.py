from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 6, 11]))
    .select("id", "store_name", "sqft")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Singapore", "Tokyo", "Bangkok"]))
        .select("id", "store_name", "sqft")
    )
)
