from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("supplier_name_title", F.initcap(F.trim("supplier_name")))
    .withColumn("city_country_key", F.concat_ws("-", "city", "country"))
)
