from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 5139)
    .select("id", "city", "sqft", F.coalesce("sqft", F.lit(0)).alias("sqft_filled"))
)
