from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Fashion", "Grocery"]) & (F.col("brand") != "Umbrella") & F.col("brand").isNotNull())
    .select("id", "tags")
    .orderBy(F.col("id").desc())
    .limit(5)
)
