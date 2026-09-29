from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Tokyo"]) & F.col("country").isin(["SG", "TH"]))
    .orderBy("id")
    .limit(1)
)
