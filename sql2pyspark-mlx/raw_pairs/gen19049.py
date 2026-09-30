from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Li%") | F.col("city").isin(["Singapore", "Tokyo", "Chiang Mai"]))
)
