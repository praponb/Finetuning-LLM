from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "London") & F.col("supplier_name").like("P%") & (F.col("city") != "Tokyo"))
    .select("id", "city", "country", "rating")
    .orderBy("city")
)
