from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "TOKYO TRADERS") & (F.col("city") == "London"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
    .orderBy(F.col("id").desc(), "supplier_name")
)
