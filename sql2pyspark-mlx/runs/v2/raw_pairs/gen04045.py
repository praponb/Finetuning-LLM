from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("country") == "DE")

agg = (
    filtered_suppliers
    .groupBy("city")
    .agg(F.countDistinct("rating").alias("unique_countrys"))
)

result = agg.filter(F.col("unique_countrys") >= 4)
