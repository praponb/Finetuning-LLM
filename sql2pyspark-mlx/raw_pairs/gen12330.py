from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.count("*").alias("num_inventory"),
        F.max("quantity").alias("max_quantity"),
    )
)
