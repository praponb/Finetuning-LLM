from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("p.brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
    .orderBy("country", F.col("umbrella_count").desc())
)
