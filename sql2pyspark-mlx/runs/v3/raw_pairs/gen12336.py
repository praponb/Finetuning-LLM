from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.lower(F.trim("store_name")) == "old town") & (F.col("city").isin(["London", "Tokyo"]) | F.col("city").isin(["Bangkok", "Chiang Mai", "Singapore"])))
    .select(
        "id",
        "region",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
        F.ceil("sqft").alias("sqft_whole"),
    )
)
