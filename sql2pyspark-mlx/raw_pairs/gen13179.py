from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "quantity",
        F.ceil("quantity").alias("quantity_whole"),
        F.ceil("quantity").alias("quantity_whole_2"),
    )
)
