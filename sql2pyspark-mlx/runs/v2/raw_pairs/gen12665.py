from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "TH", "DE"]) & F.col("rating").between(1.8, 4.0))
    .select("supplier_name", "city", "country")
    .orderBy("supplier_name")
)
