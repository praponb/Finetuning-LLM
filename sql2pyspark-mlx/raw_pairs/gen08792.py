from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.6, 3.8))
    .select(F.lit("supplier").alias("source"), "city")
    .unionByName(df_stores.select(F.lit("store").alias("source"), "city"))
    .orderBy("city")
)
