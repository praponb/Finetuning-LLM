from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "US", "TH"]) | (F.col("country") == "SG"))
    .select(
        "id",
        "rating",
        "country",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)
