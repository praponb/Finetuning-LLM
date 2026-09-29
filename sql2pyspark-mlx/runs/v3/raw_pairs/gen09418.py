from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & F.col("supplier_name").like("%Import%") & F.col("city").isin(["Tokyo", "Berlin", "Singapore"]))
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.5, "High").when(F.col("rating") >= 1.8, "Medium").otherwise("Low"))
    .withColumn("city_lower", F.lower("city"))
)
