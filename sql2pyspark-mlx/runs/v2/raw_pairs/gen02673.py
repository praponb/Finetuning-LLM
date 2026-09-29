from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "JP") & (F.length("supplier_name") > 7))
    .select("id", "country", "city", "rating")
    .orderBy("city")
)
