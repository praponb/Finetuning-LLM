from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pa%") & ~F.col("city").isin(["New York", "Tokyo"]) & F.col("supplier_name").like("P%"))
    .select("id", "country", "rating", "supplier_name")
    .orderBy("supplier_name", "id")
    .limit(5)
)
