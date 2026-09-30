from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%EuroGoo%") & (F.col("country") == "TH") & F.col("city").isin(["New York", "Bangkok"]))
    .groupBy("country", "city")
    .agg(F.sum("rating").alias("total_rating"))
)
