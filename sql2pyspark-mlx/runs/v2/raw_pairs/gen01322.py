from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "rating")
    .orderBy("rating", F.col("id").desc())
)
