from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("segment", F.round((F.col("age") * 0.85), 2).alias("age_scaled"))
)
