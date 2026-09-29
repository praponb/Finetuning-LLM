from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "UK"]))
    .select("country")
    .distinct()
    .orderBy("country")
    .limit(10)
)
