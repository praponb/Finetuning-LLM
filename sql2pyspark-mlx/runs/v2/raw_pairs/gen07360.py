from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .withColumn("supplier_name_city_key", F.concat_ws("-", "supplier_name", "city"))
    .orderBy("city")
)
