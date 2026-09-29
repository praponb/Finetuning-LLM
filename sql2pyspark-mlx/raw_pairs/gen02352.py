from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.col("opened_date") <= "2025-03-01"))
    .select("opened_date", "region", F.substring("city", 1, 2).alias("city_prefix"))
)
