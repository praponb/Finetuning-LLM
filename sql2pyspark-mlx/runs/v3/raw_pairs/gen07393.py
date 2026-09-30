from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("s.city"),
        F.when(F.col("s.sqft") >= 13018, "High").when(F.col("s.sqft") >= 8132, "Medium").otherwise("Low").alias("sqft_level"),
    )
    .orderBy(F.col("id").desc(), "sqft_level")
)
