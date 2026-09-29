from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & F.col("rating").between(2.0, 3.2))
    .withColumn("country_city", F.concat("country", F.lit(" - "), "city"))
)

agg = (
    base
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
    )
)

result = agg.filter(F.col("de_count") > 5)
