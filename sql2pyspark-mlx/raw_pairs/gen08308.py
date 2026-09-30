from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(12303, 13007) | (F.upper(F.trim("store_name")) == "MALL CENTRAL"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("min_opened_date")
)
