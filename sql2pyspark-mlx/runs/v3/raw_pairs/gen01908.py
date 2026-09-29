from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select("city", "country", F.substring("city", 1, 2).alias("city_prefix"))
    .orderBy("city")
)
