from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_int", F.col("rating").cast("int"))
    .withColumn("country_supplier_name", F.concat("country", F.lit(" - "), "supplier_name"))
    .orderBy(F.col("rating").desc(), "country", "id")
    .limit(5)
)
