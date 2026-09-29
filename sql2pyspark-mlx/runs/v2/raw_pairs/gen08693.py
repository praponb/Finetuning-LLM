from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
    .withColumn("supplier_name_city_key", F.concat_ws("/", "supplier_name", "city"))
)
