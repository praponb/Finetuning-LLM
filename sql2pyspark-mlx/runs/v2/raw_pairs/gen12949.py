from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "Chiang Mai") & (F.col("rating") > 3.3))
    .withColumn("rating_band", F.when(F.col("rating") >= 3.9, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low"))
    .orderBy("supplier_name")
)
