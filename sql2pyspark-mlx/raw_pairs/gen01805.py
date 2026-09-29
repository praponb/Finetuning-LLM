from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
)
