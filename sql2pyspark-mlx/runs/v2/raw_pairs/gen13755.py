from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.9) & (F.col("rating") < 2.2))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)
