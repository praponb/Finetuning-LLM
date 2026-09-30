from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_whole", F.floor("rating"))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)
