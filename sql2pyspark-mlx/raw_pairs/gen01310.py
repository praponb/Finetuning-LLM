from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 250) & ~F.col("brand").isin(["Globex", "Umbrella", "Initech"]))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)
