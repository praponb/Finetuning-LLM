from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .select("rating", F.col("rating").cast("int").alias("rating_int"))
)
