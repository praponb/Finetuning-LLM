from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select("id", "country")
)
