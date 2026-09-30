from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("Ol%"))
    .agg(F.sum("sqft").alias("total_sqft"))
)
