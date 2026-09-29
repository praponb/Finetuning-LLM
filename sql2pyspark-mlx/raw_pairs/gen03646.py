from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Berlin", "Tokyo"]))
    .withColumn("rating_adjusted", F.round((F.col("rating") * 1.07), 2))
    .withColumn("supplier_name_city", F.concat("supplier_name", F.lit(" - "), "city"))
)
