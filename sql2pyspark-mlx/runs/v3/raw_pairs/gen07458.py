from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .withColumn("rating_adjusted", F.round((F.col("rating") * 100), 2))
)
