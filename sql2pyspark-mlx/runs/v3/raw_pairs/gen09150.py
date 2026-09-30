from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["US", "SG"]))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
    .filter(F.col("th_rating") > 59)
    .drop("th_rating")
)
