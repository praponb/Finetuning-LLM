from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("supplier_name").isNotNull() & F.col("city").isin(["Chiang Mai", "Berlin"])) | F.col("city").isin(["New York", "Tokyo", "Bangkok"]))
    .groupBy("city", "country")
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .orderBy(F.col("uk_count").desc())
)
