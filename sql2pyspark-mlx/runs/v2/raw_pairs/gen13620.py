from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.1) & ((F.col("rating") <= 2.7) | (F.col("city") == "Singapore")))
    .select("city")
    .orderBy("city")
)
