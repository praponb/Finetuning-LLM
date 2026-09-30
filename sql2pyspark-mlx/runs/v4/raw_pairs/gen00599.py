from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("quantity") < 159)
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

result = (
    base
    .select("id", "store_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id").isin([3, 4, 5, 6]))
        .select("id", "store_id")
    )
)
