from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "North") & (F.col("sqft") == 13018) & (F.col("region") == "Central"))
    .select("region", "opened_date", "city", F.round("sqft", 2).alias("sqft_rounded"))
    .orderBy(F.col("opened_date").desc())
)
