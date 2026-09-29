from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(F.col("city").isNotNull() & (F.col("city") != "Tokyo")))
    .withColumn("is_uk", F.when(F.col("country") == "UK", 1).otherwise(0))
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(", "), "country"))
)
