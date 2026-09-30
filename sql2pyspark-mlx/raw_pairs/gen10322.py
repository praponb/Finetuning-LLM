from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai"]) & ~F.col("supplier_name").like("%Imports%"))
    .withColumn("supplier_name_normalized", F.upper(F.trim("supplier_name")))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
    .orderBy(F.col("supplier_name").desc(), "rating")
)
