from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Bangkok", "Chiang Mai"]) & ((F.col("city") == "Berlin") | F.col("city").isin(["London", "Tokyo"])))
    .select("id", "city", F.round("rating", 0).alias("rating_rounded"))
)
