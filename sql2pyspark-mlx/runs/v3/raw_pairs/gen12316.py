from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country").isin(["US", "UK", "TH"]) & (F.col("rating") < 2.9)) | ~F.col("city").isin(["Bangkok", "Chiang Mai", "Berlin"]))
    .select("id", "rating", "city", "country")
)
