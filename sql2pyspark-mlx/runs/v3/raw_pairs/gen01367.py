from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.6) & F.col("city").isin(["London", "Tokyo"]))
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(" "), "country"))
    .orderBy(F.col("rating").desc())
)
