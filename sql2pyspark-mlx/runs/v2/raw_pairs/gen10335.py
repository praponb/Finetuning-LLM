from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .select("id", "city", F.col("rating").cast("int").alias("rating_int"))
)
