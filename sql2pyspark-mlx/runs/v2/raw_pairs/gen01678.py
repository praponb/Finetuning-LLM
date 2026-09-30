from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "store_id", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
    .orderBy(F.col("store_id").desc(), F.col("quantity_filled").desc())
)
