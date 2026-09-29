from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
    .orderBy("rating_filled")
)
