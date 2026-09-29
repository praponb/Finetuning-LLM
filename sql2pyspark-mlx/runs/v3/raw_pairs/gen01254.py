from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco") & (F.col("cost") <= 440))
    .select("brand", "cost")
)
