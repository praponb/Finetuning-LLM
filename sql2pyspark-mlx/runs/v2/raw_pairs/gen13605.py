from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.8)
    .withColumn("supplier_name_filled", F.coalesce("supplier_name", F.lit("N/A")))
    .withColumn("city_supplier_name_key", F.concat_ws("-", "city", "supplier_name"))
    .orderBy(F.col("country").desc(), "id")
    .limit(10)
)
