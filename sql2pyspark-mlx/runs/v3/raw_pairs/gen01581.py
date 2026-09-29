from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", "_"))
    .withColumn("rating_rounded", F.round("rating", 2))
)
