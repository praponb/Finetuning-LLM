from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "SG", "JP"]) & F.col("rating").between(2.6, 3.6))
)

agg = (
    filtered_suppliers
    .groupBy("city")
    .agg(F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"))
)

result = (
    agg
    .filter(F.col("de_count") < 43)
    .orderBy(F.col("de_count").desc(), "city")
)
