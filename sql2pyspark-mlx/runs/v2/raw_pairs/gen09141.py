from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.3)
    .select("id", "country", F.coalesce("rating", F.lit(0)).alias("rating_filled"))
    .orderBy("country")
)
