from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Berlin", "Tokyo"]) & (F.col("segment") != "Corporate"))
    .select("id", "name")
    .orderBy("id")
)
