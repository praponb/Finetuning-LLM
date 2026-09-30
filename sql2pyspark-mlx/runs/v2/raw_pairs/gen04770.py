from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Singapore", "Berlin"]))
    .orderBy("id")
)
