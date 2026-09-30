from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Ol%") & (F.upper(F.trim("store_name")) == "RIVERSIDE"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.max("opened_date").alias("last_opened_date"),
    )
)
